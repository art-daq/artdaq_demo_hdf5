#ifndef artdaq_ArtModules_detail_HDFFileReader_hh
#define artdaq_ArtModules_detail_HDFFileReader_hh

#include "artdaq/DAQdata/Globals.hh"

#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"
#include "artdaq-demo-hdf5/HDF5/MakeDatasetPlugin.hh"

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
		 * \brief The DefaultFragmentTypeTranslator class provides default behavior
		 *        for experiment-specific customizations in HDFFileReader.
		 */
class DefaultFragmentTypeTranslator
{
public:
	DefaultFragmentTypeTranslator()
	    : type_map_() {}
	virtual ~DefaultFragmentTypeTranslator() = default;

	/**
			 * \brief Sets the basic types to be translated.  (Should not include "container" types.)
			 */
	virtual void SetBasicTypes(std::map<Fragment::type_t, std::string> const& type_map)
	{
		type_map_ = type_map;
	}

	/**
			 * \brief Adds an additional type to be translated.
			 */
	virtual void AddExtraType(artdaq::Fragment::type_t type_id, std::string type_name)
	{
		type_map_[type_id] = type_name;
	}

	/**
			 * \brief Returns the basic translation for the specified type.  Defaults to the specified
			 *        unidentified_instance_name if no translation can be found.
			 */
	virtual std::string GetInstanceNameForType(artdaq::Fragment::type_t type_id, std::string unidentified_instance_name)
	{
		if (type_map_.count(type_id) > 0) { return type_map_[type_id]; }
		return unidentified_instance_name;
	}

	/**
			 * \brief Returns the full set of product instance names which may be present in the data, based on
			 *        the types that have been specified in the SetBasicTypes() and AddExtraType() methods.  This
			 *        *does* include "container" types, if the container type mapping is part of the basic types.
			 */
	virtual std::set<std::string> GetAllProductInstanceNames()
	{
		std::set<std::string> output;
		for (const auto& map_iter : type_map_)
		{
			std::string instance_name = map_iter.second;
			if (!output.count(instance_name))
			{
				output.insert(instance_name);
				TLOG_TRACE("DefaultFragmentTypeTranslator") << "Adding product instance name \"" << map_iter.second
				                                            << "\" to list of expected names";
			}
		}

		auto container_type = type_map_.find(Fragment::type_t(artdaq::Fragment::ContainerFragmentType));
		if (container_type != type_map_.end())
		{
			std::string container_type_name = container_type->second;
			std::set<std::string> tmp_copy = output;
			for (const auto& set_iter : tmp_copy)
			{
				output.insert(container_type_name + set_iter);
			}
		}

		return output;
	}

	/**
			 * \brief Returns the product instance name for the specified fragment, based on the types that have
			 *        been specified in the SetBasicTypes() and AddExtraType() methods.  This *does* include the
			 *        use of "container" types, if the container type mapping is part of the basic types.  If no
			 *        mapping is found, the specified unidentified_instance_name is returned.
			 */
	virtual std::pair<bool, std::string>
	GetInstanceNameForFragment(artdaq::Fragment const& fragment, std::string unidentified_instance_name)
	{
		auto type_map_end = type_map_.end();
		bool success_code = true;
		std::string instance_name;

		auto primary_type = type_map_.find(fragment.type());
		if (primary_type != type_map_end)
		{
			instance_name = primary_type->second;
			if (fragment.type() == artdaq::Fragment::ContainerFragmentType)
			{
				artdaq::ContainerFragment cf(fragment);
				auto containedFragmentType = cf.fragment_type();
				TLOG(TLVL_TRACE)
				    << "Fragment type is Container, determining label for contained type " << containedFragmentType;
				auto contained_type = type_map_.find(containedFragmentType);
				if (contained_type != type_map_end)
				{
					instance_name += contained_type->second;
				}
			}
		}
		else
		{
			instance_name = unidentified_instance_name;
			success_code = false;
		}

		return std::make_pair(success_code, instance_name);
	}

protected:
	std::map<Fragment::type_t, std::string> type_map_;  ///< Map relating Fragment Type to strings
};

/**
 * \brief The HDFFileReader is a class which implements the methods needed by art::Source
 */
template<std::map<artdaq::Fragment::type_t, std::string> getDefaultTypes() = artdaq::Fragment::MakeSystemTypeMap,
         class FTT = artdaq::detail::DefaultFragmentTypeTranslator>
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
	std::string unidentified_instance_name;                ///< The name to use for unknown Fragment types
	bool shutdownMsgReceived;                              ///< Whether a shutdown message has been received
	size_t bytesRead;                                      ///< running total of number of bytes received
	std::chrono::steady_clock::time_point last_read_time;  ///< Time last read was completed
	                                                       // std::unique_ptr<SharedMemoryManager> data_shm; ///< SharedMemoryManager containing data
	                                                       // std::unique_ptr<SharedMemoryManager> broadcast_shm; ///< SharedMemoryManager containing broadcasts (control
	                                                       // Fragments)
	unsigned readNext_calls_;                              ///< The number of times readNext has been called
	FTT translator_;                                       ///< An instance of the template parameter FragmentTypeTranslator that translates Fragment Type IDs to strings for creating ROOT TTree Branches
	std::unique_ptr<artdaq::hdf5::FragmentDataset> inputFile_;

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
	    , unidentified_instance_name("unidentified")
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
		inputFile_ = artdaq::hdf5::MakeDatasetPlugin(ps, "dataset");

		help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, unidentified_instance_name);

		// Workaround for #22979
		help.reconstitutes<Fragments, art::InRun>(pretend_module_name, unidentified_instance_name);
		help.reconstitutes<Fragments, art::InSubRun>(pretend_module_name, unidentified_instance_name);

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

		translator_.SetBasicTypes(getDefaultTypes());
		auto extraTypes = ps.get<std::vector<std::pair<Fragment::type_t, std::string>>>("fragment_type_map", std::vector<std::pair<Fragment::type_t, std::string>>());
		for (auto it = extraTypes.begin(); it != extraTypes.end(); ++it)
		{
			translator_.AddExtraType(it->first, it->second);
		}
		std::set<std::string> instance_names = translator_.GetAllProductInstanceNames();
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
		                            << translator_.GetInstanceNameForType(firstFragmentType, unidentified_instance_name) << ")";
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

		auto evtHeader = inputFile_->GetEventHeader(eventMap.begin()->second->at(0).sequenceID());
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
				    translator_.GetInstanceNameForFragment(frag, unidentified_instance_name);
				std::string label = instance_name_result.second;
				if (!instance_name_result.first)
				{
					TLOG_WARNING("HDFFileReader")
					    << "UnknownFragmentType: The product instance name mapping for fragment type \"" << ((int)type_code)
					    << "\" is not known. Fragments of this "
					    << "type will be stored in the event with an instance name of \"" << unidentified_instance_name << "\".";
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
