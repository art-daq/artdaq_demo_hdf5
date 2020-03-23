#ifndef artdaq_ArtModules_detail_HDFFileReader_hh
#define artdaq_ArtModules_detail_HDFFileReader_hh

#include "artdaq/DAQdata/Globals.hh"

#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"
#include "artdaq-demo-hdf5/HDF5/MakeDatasetPlugin.hh"
#include "artdaq/ArtModules/ArtdaqFragmentNamingService.h"

#include "art/Framework/Core/Frameworkfwd.h"

#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/IO/Sources/put_product_in_principal.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "canvas/Persistency/Provenance/FileFormatVersion.h"
#include "fhiclcpp/ParameterSet.h"

#include <sys/time.h>
#include <map>
#include <string>

namespace artdaq {
namespace detail {

/**
 * \brief The HDFFileReader is a class which implements the methods needed by art::Source
 */
struct HDFFileReader
{
	/**
   * \brief Copy Constructor is deleted
   */
	HDFFileReader(HDFFileReader const&) = delete;

	/**
   * \brief Copy Assignment operator is deleted
   * \return HDFFileReader copy
   */
	HDFFileReader& operator=(HDFFileReader const&) = delete;

	art::SourceHelper const& pmaker;                       ///< An art::SourceHelper instance
	std::string pretend_module_name;                       ///< The module name to store data under
	bool shutdownMsgReceived;                              ///< Whether a shutdown message has been received
	size_t bytesRead;                                      ///< running total of number of bytes received
	std::chrono::steady_clock::time_point last_read_time;  ///< Time last read was completed
	unsigned readNext_calls_;                              ///< The number of times readNext has been called
	std::unique_ptr<artdaq::hdf5::FragmentDataset> inputFile_; ///< The Dataset plugin which this input source will be reading from

	/**
   * \brief HDFFileReader Constructor
   * \param ps ParameterSet used for configuring HDFFileReader
   * \param help art::ProductRegistryHelper which is used to inform art about different Fragment types
   * \param pm art::SourceHelper used to initalize the SourceHelper member
   *
   * \verbatim
   * HDFFileReader accepts the following Parameters:
   * "raw_data_label" (Default: "daq"): The label to use for all raw data
   * "shared_memory_key" (Default: 0xBEE7): The key for the shared memory segment
   * \endverbatim
   */
	HDFFileReader(fhicl::ParameterSet const& ps,
	              art::ProductRegistryHelper& help,
	              art::SourceHelper const& pm)
	    : pmaker(pm)
	    , pretend_module_name(ps.get<std::string>("raw_data_label", "daq"))
	    , shutdownMsgReceived(false)
	    , bytesRead(0)
	    , last_read_time(std::chrono::steady_clock::now())
	    , readNext_calls_(0)
	{
#if 0
		volatile bool keep_looping = true;
		while (keep_looping)
		{
			usleep(10000);
		}
#endif
		art::ServiceHandle<ArtdaqFragmentNamingServiceInterface> translator;
		inputFile_ = artdaq::hdf5::MakeDatasetPlugin(ps, "dataset");

		help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, translator->GetUnidentifiedInstanceName());

		// Workaround for #22979
		help.reconstitutes<Fragments, art::InRun>(pretend_module_name, translator->GetUnidentifiedInstanceName());
		help.reconstitutes<Fragments, art::InSubRun>(pretend_module_name, translator->GetUnidentifiedInstanceName());

		try
		{
			if (metricMan)
			{
				metricMan->initialize(ps.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()), "art");
				metricMan->do_start();
			}
		}
		catch (...)
		{
			artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::no, "Error loading metrics in HDFFileReader()");
		}

		std::set<std::string> instance_names = translator->GetAllProductInstanceNames();
		for (const auto& set_iter : instance_names)
		{
			help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, set_iter);
		}

		TLOG_INFO("HDFFileReader") << "HDFFileReader initialized with ParameterSet: " << ps.to_string();
	}

#if ART_HEX_VERSION < 0x30000
	/**
   * \brief HDFFileReader Constructor
   * \param ps ParameterSet used for configuring HDFFileReader
   * \param help art::ProductRegistryHelper which is used to inform art about different Fragment types
   * \param pm art::SourceHelper used to initalize the SourceHelper member
   *
   * This constructor calls the three-parameter constructor, the art::MasterProductRegistry parameter is discarded.
   */
	HDFFileReader(fhicl::ParameterSet const& ps, art::ProductRegistryHelper& help, art::SourceHelper const& pm,
	              art::MasterProductRegistry&)
	    : HDFFileReader(ps, help, pm) {}
#endif

	/**
   * \brief HDFFileReader destructor
   */
	virtual ~HDFFileReader() {}

	/**
   * \brief Emulate closing a file. No-Op.
   */
	void closeCurrentFile() {}

	/**
   * \brief Emulate opening a file
   * \param[out] fb art::FileBlock object
   */
	void readFile(std::string const&, art::FileBlock*& fb)
	{
		TLOG_ARB(5, "HDFFileReader") << "readFile enter/start";
		fb = new art::FileBlock(art::FileFormatVersion(1, "RawEvent2011"), "nothing");
	}

	/**
   * \brief Whether more data is expected from the HDFFileReader
   * \return True unless a shutdown message has been received in readNext
   */
	bool hasMoreData() const
	{
		TLOG(TLVL_DEBUG, "HDFFileReader") << "hasMoreData returning " << std::boolalpha << !shutdownMsgReceived;
		return (!shutdownMsgReceived);
	}

	/**
   * \brief Dequeue a RawEvent and declare its Fragment contents to art, creating
   * Run, SubRun, and EventPrincipal objects as necessary
   * \param[in] inR Input art::RunPrincipal
   * \param[in] inSR Input art::SubRunPrincipal
   * \param[out] outR Output art::RunPrincipal
   * \param[out] outSR  Output art::SubRunPrincipal
   * \param[out] outE Output art::EventPrincipal
   * \return Whether an event was returned
   */
	bool readNext(art::RunPrincipal* const& inR, art::SubRunPrincipal* const& inSR, art::RunPrincipal*& outR,
	              art::SubRunPrincipal*& outSR, art::EventPrincipal*& outE)
	{
		TLOG_DEBUG("HDFFileReader") << "readNext BEGIN";
		art::ServiceHandle<ArtdaqFragmentNamingServiceInterface> translator;

		// Establish default 'results'
		outR = 0;
		outSR = 0;
		outE = 0;
		// Try to get an event from the queue. We'll continuously loop, either until:
		//   1) we have read a RawEvent off the queue, or
		//   2) we have timed out, AND we are told the when we timeout we
		//      should stop.
		// In any case, if we time out, we emit an informational message.

		if (shutdownMsgReceived)
		{
			TLOG_INFO("HDFFileReader") << "Shutdown Message received, returning false (should exit art)";
			return false;
		}

		auto read_start_time = std::chrono::steady_clock::now();

		std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> eventMap = inputFile_->readNextEvent();
		if (eventMap.size() == 0)
		{
			TLOG_ERROR("HDFFileReader") << "No data received, either because of incompatible plugin or end of file. Returning false (should exit art)";
			shutdownMsgReceived = true;
			return false;
		}

		auto got_event_time = std::chrono::steady_clock::now();
		auto firstFragmentType = eventMap.begin()->first;
		TLOG_DEBUG("HDFFileReader") << "First Fragment type is " << (int)firstFragmentType << " ("
		                            << translator->GetInstanceNameForType(firstFragmentType) << ")";
		// We return false, indicating we're done reading, if:
		//   1) we did not obtain an event, because we timed out and were
		//      configured NOT to keep trying after a timeout, or
		//   2) the event we read was the end-of-data marker: a null
		//      pointer
		if (firstFragmentType == Fragment::EndOfDataFragmentType)
		{
			TLOG_DEBUG("HDFFileReader") << "Received shutdown message, returning false";
			shutdownMsgReceived = true;
			return false;
		}

		auto evtHeader = inputFile_->getEventHeader(eventMap.begin()->second->at(0).sequenceID());
		if (evtHeader == nullptr)
		{
			TLOG_DEBUG("HDFFileReader") << "Did not receive Event Header for sequence ID " << eventMap.begin()->second->at(0).sequenceID() << ", skipping event";
			return true;
		}
		else
		{
			TLOG_TRACE("HDFFileReader") << "EventHeader has Run " << evtHeader->run_id << ", SubRun " << evtHeader->subrun_id << ", Event " << evtHeader->event_id << ", SeqID " << evtHeader->sequence_id << ", IsComplete " << evtHeader->is_complete;
		}
		// Check the number of fragments in the RawEvent.  If we have a single
		// fragment and that fragment is marked as EndRun or EndSubrun we'll create
		// the special principals for that.
		art::Timestamp currentTime = 0;
#if 0
				art::TimeValue_t lo_res_time = time(0);
				TLOG_ARB(15, "HDFFileReader") << "lo_res_time = " << lo_res_time;
				currentTime = ((lo_res_time & 0xffffffff) << 32);
#endif
		timespec hi_res_time;
		int retcode = clock_gettime(CLOCK_REALTIME, &hi_res_time);
		TLOG_ARB(15, "HDFFileReader") << "hi_res_time tv_sec = " << hi_res_time.tv_sec
		                              << " tv_nsec = " << hi_res_time.tv_nsec << " (retcode = " << retcode << ")";
		if (retcode == 0)
		{
			currentTime = ((hi_res_time.tv_sec & 0xffffffff) << 32) | (hi_res_time.tv_nsec & 0xffffffff);
		}
		else
		{
			TLOG_ERROR("HDFFileReader")
			    << "Unable to fetch a high-resolution time with clock_gettime for art::Event Timestamp. "
			    << "The art::Event Timestamp will be zero for event " << evtHeader->event_id;
		}

		// make new run if inR is 0 or if the run has changed
		if (inR == 0 || inR->run() != evtHeader->run_id)
		{
			outR = pmaker.makeRunPrincipal(evtHeader->run_id, currentTime);
		}

		if (firstFragmentType == Fragment::EndOfRunFragmentType)
		{
			art::EventID const evid(art::EventID::flushEvent());
			outR = pmaker.makeRunPrincipal(evid.runID(), currentTime);
			outSR = pmaker.makeSubRunPrincipal(evid.subRunID(), currentTime);
			outE = pmaker.makeEventPrincipal(evid, currentTime);
			return true;
		}
		else if (firstFragmentType == Fragment::EndOfSubrunFragmentType)
		{
			// Check if inR == 0 or is a new run
			if (inR == 0 || inR->run() != evtHeader->run_id)
			{
				outSR = pmaker.makeSubRunPrincipal(evtHeader->run_id, evtHeader->subrun_id, currentTime);
#if ART_HEX_VERSION > 0x30000
				art::EventID const evid(art::EventID::flushEvent(outSR->subRunID()));
#else
				art::EventID const evid(art::EventID::flushEvent(outSR->id()));
#endif
				outE = pmaker.makeEventPrincipal(evid, currentTime);
			}
			else
			{
				// If the previous subrun was neither 0 nor flush and was identical with the current
				// subrun, then it must have been associated with a data event.  In that case, we need
				// to generate a flush event with a valid run but flush subrun and event number in order
				// to end the subrun.
#if ART_HEX_VERSION > 0x30000
				if (inSR != 0 && !inSR->subRunID().isFlush() && inSR->subRun() == evtHeader->subrun_id)
#else
				if (inSR != 0 && !inSR->id().isFlush() && inSR->subRun() == evtHeader->subrun_id)
#endif
				{
#if ART_HEX_VERSION > 0x30000
					art::EventID const evid(art::EventID::flushEvent(inR->runID()));
#else
					art::EventID const evid(art::EventID::flushEvent(inR->id()));
#endif
					outSR = pmaker.makeSubRunPrincipal(evid.subRunID(), currentTime);
					outE = pmaker.makeEventPrincipal(evid, currentTime);
					// If this is either a new or another empty subrun, then generate a flush event with
					// valid run and subrun numbers but flush event number
					//} else if(inSR==0 || inSR->id().isFlush()){
				}
				else
				{
					outSR = pmaker.makeSubRunPrincipal(evtHeader->run_id, evtHeader->subrun_id, currentTime);
#if ART_HEX_VERSION > 0x30000
					art::EventID const evid(art::EventID::flushEvent(outSR->subRunID()));
#else
					art::EventID const evid(art::EventID::flushEvent(outSR->id()));
#endif
					outE = pmaker.makeEventPrincipal(evid, currentTime);
					// Possible error condition
					//} else {
				}
				outR = 0;
			}
			// outputFileCloseNeeded = true;
			return true;
		}

		// make new subrun if inSR is 0 or if the subrun has changed
		art::SubRunID subrun_check(evtHeader->run_id, evtHeader->subrun_id);
#if ART_HEX_VERSION > 0x30000
		if (inSR == 0 || subrun_check != inSR->subRunID())
		{
#else
		if (inSR == 0 || subrun_check != inSR->id())
		{
#endif
			outSR = pmaker.makeSubRunPrincipal(evtHeader->run_id, evtHeader->subrun_id, currentTime);
		}
		outE = pmaker.makeEventPrincipal(evtHeader->run_id, evtHeader->subrun_id, evtHeader->event_id, currentTime);

		// insert the Fragments of each type into the EventPrincipal
		for (auto& fragmentTypePair : eventMap)
		{
			auto type_code = fragmentTypePair.first;
			TLOG_TRACE("HDFFileReader") << "Before GetFragmentsByType call, type is " << (int)type_code;
			TLOG_TRACE("HDFFileReader") << "After GetFragmentsByType call, number of fragments is " << fragmentTypePair.second->size();

			std::unordered_map<std::string, std::unique_ptr<Fragments>> derived_fragments;
			for (auto& frag : *fragmentTypePair.second)
			{
				bytesRead += frag.sizeBytes();

				std::pair<bool, std::string> instance_name_result =
				    translator->GetInstanceNameForFragment(frag);
				std::string label = instance_name_result.second;
				if (!instance_name_result.first)
				{
					TLOG_WARNING("HDFFileReader")
					    << "UnknownFragmentType: The product instance name mapping for fragment type \"" << ((int)type_code)
					    << "\" is not known. Fragments of this "
					    << "type will be stored in the event with an instance name of \"" << label << "\".";
				}
				if (!derived_fragments.count(label))
				{
					derived_fragments[label] = std::make_unique<Fragments>();
				}
				auto fragSize = frag.size();
				TLOG_TRACE("HDFFileReader") << "Moving Fragment with size " << fragSize << " from type map (type=" << type_code << ") to derived map label=" << label;
				derived_fragments[label]->emplace_back(std::move(frag));
			}
			TLOG_TRACE("HDFFileReader") << "Placing derived fragments in outE";
			for (auto& type : derived_fragments)
			{
				put_product_in_principal(std::move(type.second),
				                         *outE,
				                         pretend_module_name,
				                         type.first);
			}
		}
		TLOG_TRACE("HDFFileReader") << "After putting fragments in event";

		auto read_finish_time = std::chrono::steady_clock::now();
		TLOG_ARB(10, "HDFFileReader") << "readNext: bytesRead=" << bytesRead << " metricMan=" << (void*)metricMan.get();
		if (metricMan)
		{
			metricMan->sendMetric("Avg Processing Time", artdaq::TimeUtils::GetElapsedTime(last_read_time, read_start_time),
			                      "s", 2, MetricMode::Average);
			metricMan->sendMetric("Avg Input Wait Time", artdaq::TimeUtils::GetElapsedTime(read_start_time, got_event_time),
			                      "s", 3, MetricMode::Average);
			metricMan->sendMetric("Avg Read Time", artdaq::TimeUtils::GetElapsedTime(got_event_time, read_finish_time), "s",
			                      3, MetricMode::Average);
			metricMan->sendMetric("bytesRead", bytesRead, "B", 3, MetricMode::LastPoint);
		}

		TLOG_TRACE("HDFFileReader") << "Returning from readNext";
		last_read_time = std::chrono::steady_clock::now();
		return true;
	}
};
}  // namespace detail
}  // namespace artdaq

#endif /* artdaq_ArtModules_detail_HDFFileReader_hh */
