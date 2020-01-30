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

#include <stdio.h>
#include <unistd.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
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
	virtual ~HDFFileOutput();

private:
	void beginJob() override;

	void endJob() override;

	void write(EventPrincipal&) override;

	void writeRun(RunPrincipal&) override{};
	void writeSubRun(SubRunPrincipal&) override{};

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
	using RawEvent = artdaq::Fragments;
	using RawEvents = std::vector<RawEvent>;
	using RawEventHandle = art::Handle<RawEvent>;
	using RawEventHeaderHandle = art::Handle<artdaq::detail::RawEventHeader>;

	auto result_handles = std::vector<art::GroupQueryResult>();
	auto const& wrapped = art::WrappedTypeID::make<RawEvent>();
#if ART_HEX_VERSION >= 0x30000
	ModuleContext const mc{moduleDescription()};
	ProcessTag const processTag{"", mc.moduleDescription().processName()};

	result_handles = ep.getMany(mc, wrapped, art::MatchAllSelector{}, processTag);
#else
	result_handles = ep.getMany(wrapped, art::MatchAllSelector{});
#endif
	auto hdr_found = false;
	auto sequence_id = artdaq::Fragment::InvalidSequenceID;

	for (auto const& result_handle : result_handles)
	{
		auto const raw_event_handle = RawEventHandle(result_handle);

		if (raw_event_handle.isValid())
		{
			for (auto const& fragment : *raw_event_handle)
			{
				sequence_id = fragment.sequenceID();
				auto fragid_id = fragment.fragmentID();
				auto addr = reinterpret_cast<const void*>(fragment.headerBeginBytes());
				auto size_bytes = fragment.sizeBytes();
				TLOG(TLVL_TRACE) << "HDFFileOutput::write seq=" << sequence_id << " frag=" << fragid_id << " "
				                 << addr << " bytes=0x" << std::hex << size_bytes << " label=" << raw_event_handle.provenance()->productInstanceName() << " start";

				ntuple_->insert(fragment);

				TLOG(5) << "HDFFileOutput::write seq=" << sequence_id << " frag=" << fragid_id << " done errno=" << errno;
			}
		}

		auto const raw_event_header_handle = RawEventHeaderHandle(result_handle);

		if (raw_event_header_handle.isValid())
		{
			auto const& header = *raw_event_header_handle;

			auto evt_sequence_id = header.sequence_id;
			TLOG(TLVL_TRACE) << "HDFFileOutput::write header seq=" << evt_sequence_id;

			ntuple_->insert(header);

			hdr_found = true;
			TLOG(5) << "HDFFileOutput::write header seq=" << evt_sequence_id << " done errno=" << errno;
		}
	}

	if (!hdr_found)
	{
		artdaq::detail::RawEventHeader hdr(ep.run(), ep.subRun(), ep.event(), sequence_id);
		hdr.is_complete = true;

		ntuple_->insert(hdr);
	}

#if ART_HEX_VERSION < 0x30000
	fstats_.recordEvent(ep.id());
#else
	fstats_.recordEvent(ep.eventID());
#endif
	return;
}

DEFINE_ART_MODULE(art::HDFFileOutput)