#define TRACE_NAME "HDFFileOutput"

#include "artdaq-demo-hdf5/HDF5/MakeDatasetPlugin.hh"

#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/IO/FileStatsCollector.h"
#include "art/Framework/IO/PostCloseFileRenamer.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Persistency/Common/GroupQueryResult.h"
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq/DAQdata/Globals.hh"

#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

namespace art {
class HDFFileOutput;
}

using art::HDFFileOutput;
using fhicl::ParameterSet;

/**
 * \brief The HDFFileOutput module streams art Events to a binary file, bypassing ROOT
 */
class art::HDFFileOutput final : public OutputModule
{
public:
	/**
	 * \brief HDFFileOutput Constructor
	 * \param ps ParameterSet used to configure HDFFileOutput
	 *
	 * HDFFileOutput accepts the same configuration parameters as art::OutputModule.
	 * It has the same name substitution code that RootOutput uses to uniquify names.
	 *
	 * HDFFileOutput also expects the following Parameters:
	 * "fileName" (REQUIRED): Name of the file to write
	 * "directIO" (Default: false): Whether to use O_DIRECT
	 */
	explicit HDFFileOutput(ParameterSet const& ps);

	/**
	 * \brief HDFFileOutput Destructor
	 */
	~HDFFileOutput() override;

private:
	void beginJob() override;

	void endJob() override;

	void write(EventPrincipal& /*ep*/) override;

	void writeRun(RunPrincipal& /*r*/) override{};
	void writeSubRun(SubRunPrincipal& /*sr*/) override{};

private:
	std::string name_ = "HDFFileOutput";
	art::FileStatsCollector fstats_;

	std::unique_ptr<artdaq::hdf5::FragmentDataset> ntuple_;
};

art::HDFFileOutput::HDFFileOutput(ParameterSet const& ps)
    : OutputModule(ps)
    , fstats_{name_, processName()}
{
	TLOG(TLVL_DEBUG) << "Begin: HDFFileOutput::HDFFileOutput(ParameterSet const& ps)\n";
	ntuple_ = artdaq::hdf5::MakeDatasetPlugin(ps, "dataset");
	TLOG(TLVL_DEBUG)
	    << "End: HDFFileOutput::HDFFileOutput(ParameterSet const& ps)\n";
}

art::HDFFileOutput::~HDFFileOutput() { TLOG(TLVL_DEBUG) << "Begin/End: HDFFileOutput::~HDFFileOutput()\n"; }

void art::HDFFileOutput::beginJob()
{
	TLOG(TLVL_DEBUG) << "Begin: HDFFileOutput::beginJob()\n";
	TLOG(TLVL_DEBUG) << "End:   HDFFileOutput::beginJob()\n";
}

void art::HDFFileOutput::endJob()
{
	TLOG(TLVL_DEBUG) << "Begin: HDFFileOutput::endJob()\n";
	TLOG(TLVL_DEBUG) << "End:   HDFFileOutput::endJob()\n";
}

void art::HDFFileOutput::write(EventPrincipal& ep)
{
	TLOG(TLVL_TRACE) << "Begin: HDFFileOutput::write(EventPrincipal& ep)";
	using RawEvent = artdaq::Fragments;
	using RawEvents = std::vector<RawEvent>;
	using RawEventHandle = art::Handle<RawEvent>;
	using RawEventHeaderHandle = art::Handle<artdaq::detail::RawEventHeader>;

	auto hdr_found = false;
	auto sequence_id = artdaq::Fragment::InvalidSequenceID;

	TLOG(5) << "write: Retrieving event Fragments";
	{
		auto result_handles = std::vector<art::GroupQueryResult>();
		auto const& wrapped = art::WrappedTypeID::make<RawEvent>();
#if ART_HEX_VERSION >= 0x30000
		ModuleContext const mc{moduleDescription()};
		ProcessTag const processTag{"", mc.moduleDescription().processName()};

		result_handles = ep.getMany(mc, wrapped, art::MatchAllSelector{}, processTag);
#else
		result_handles = ep.getMany(wrapped, art::MatchAllSelector{});
#endif

		for (auto const& result_handle : result_handles)
		{
			auto const raw_event_handle = RawEventHandle(result_handle);

			if (raw_event_handle.isValid() && !raw_event_handle.product()->empty())
			{
				TLOG(10) << "raw_event_handle labels: branchName:" << raw_event_handle.provenance()->branchName();
				TLOG(10) << "raw_event_handle labels: friendlyClassName:" << raw_event_handle.provenance()->friendlyClassName();
				TLOG(10) << "raw_event_handle labels: inputTag:" << raw_event_handle.provenance()->inputTag();
				TLOG(10) << "raw_event_handle labels: moduleLabel:" << raw_event_handle.provenance()->moduleLabel();
				TLOG(10) << "raw_event_handle labels: processName:" << raw_event_handle.provenance()->processName();
				sequence_id = (*raw_event_handle).front().sequenceID();

				TLOG(5) << "write: Writing to dataset";
				ntuple_->insertMany(*raw_event_handle);
			}
		}
	}
	TLOG(5) << "write: Retrieving Event Header";
	{
		auto result_handles = std::vector<art::GroupQueryResult>();
		auto const& wrapped = art::WrappedTypeID::make<artdaq::detail::RawEventHeader>();
#if ART_HEX_VERSION >= 0x30000
		ModuleContext const mc{moduleDescription()};
		ProcessTag const processTag{"", mc.moduleDescription().processName()};

		result_handles = ep.getMany(mc, wrapped, art::MatchAllSelector{}, processTag);
#else
		result_handles = ep.getMany(wrapped, art::MatchAllSelector{});
#endif

		for (auto const& result_handle : result_handles)
		{
			auto const raw_event_header_handle = RawEventHeaderHandle(result_handle);

			if (raw_event_header_handle.isValid())
			{
				auto const& header = *raw_event_header_handle;

				auto evt_sequence_id = header.sequence_id;
				TLOG(TLVL_TRACE) << "HDFFileOutput::write header seq=" << evt_sequence_id;

				ntuple_->insertHeader(header);

				hdr_found = true;
				TLOG(5) << "HDFFileOutput::write header seq=" << evt_sequence_id << " done errno=" << errno;
			}
		}
	}
	if (!hdr_found)
	{
		TLOG(5) << "write: Header not found, autogenerating";
		artdaq::detail::RawEventHeader hdr(ep.run(), ep.subRun(), ep.event(), sequence_id);
		hdr.is_complete = true;

		ntuple_->insertHeader(hdr);
	}

#if ART_HEX_VERSION < 0x30000
	fstats_.recordEvent(ep.id());
#else
	fstats_.recordEvent(ep.eventID());
#endif
	TLOG(TLVL_TRACE) << "End: HDFFileOUtput::write(EventPrincipal& ep)";
}

DEFINE_ART_MODULE(art::HDFFileOutput)
