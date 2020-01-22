#define TRACE_NAME "ToyHDFFileOutput"

#include "artdaq-demo-hdf5/ArtModules/detail/ToyFragmentNtuple.hh"

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

#include "artdaq-core-demo/Overlays/ToyFragment.hh"
#include "artdaq-core-demo/Overlays/FragmentType.hh"
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
class ToyHDFFileOutput;
}

using art::ToyHDFFileOutput;
using fhicl::ParameterSet;

/**
 * \brief The ToyHDFFileOutput module streams art Events to a binary file, bypassing ROOT
 */
class art::ToyHDFFileOutput final : public OutputModule
{
public:
	/**
	 * \brief ToyHDFFileOutput Constructor
	 * \param ps ParameterSet used to configure ToyHDFFileOutput
	 *
	 * ToyHDFFileOutput accepts the same configuration parameters as art::OutputModule.
	 * It has the same name substitution code that RootOutput uses to uniquify names.
	 *
	 * ToyHDFFileOutput also expects the following Parameters:
	 * "fileName" (REQUIRED): Name of the file to write
	 * "directIO" (Default: false): Whether to use O_DIRECT
	 */
	explicit ToyHDFFileOutput(ParameterSet const& ps);

	/**
	 * \brief ToyHDFFileOutput Destructor
	 */
	virtual ~ToyHDFFileOutput();

private:
	void beginJob() override;

	void endJob() override;

	void write(EventPrincipal&) override;

	void writeRun(RunPrincipal&) override{};
	void writeSubRun(SubRunPrincipal&) override{};

private:
	std::string name_ = "ToyHDFFileOutput";
	std::string file_name_;
	art::FileStatsCollector fstats_;

	ToyFragmentNtuple toy_ntuple_;
};

art::ToyHDFFileOutput::ToyHDFFileOutput(ParameterSet const& ps)
	: OutputModule(ps)
	, file_name_(ps.get<std::string>("fileName", "/tmp/artdaqdemo.hdf5"))
	, fstats_{name_, processName()}
	, toy_ntuple_(file_name_, ps.get<size_t>("nADCsPerRow", 512))
{
	TLOG(TLVL_DEBUG) << "Begin: ToyHDFFileOutput::ToyHDFFileOutput(ParameterSet const& ps)\n";
	TLOG(TLVL_DEBUG) << "End: ToyHDFFileOutput::ToyHDFFileOutput(ParameterSet const& ps)\n";
}

art::ToyHDFFileOutput::~ToyHDFFileOutput() { TLOG(TLVL_DEBUG) << "Begin/End: ToyHDFFileOutput::~ToyHDFFileOutput()\n"; }

void art::ToyHDFFileOutput::beginJob()
{
	TLOG(TLVL_DEBUG) << "Begin: ToyHDFFileOutput::beginJob()\n";
	TLOG(TLVL_DEBUG) << "End:   ToyHDFFileOutput::beginJob()\n";
}

void art::ToyHDFFileOutput::endJob()
{
	TLOG(TLVL_DEBUG) << "Begin: ToyHDFFileOutput::endJob()\n";
	TLOG(TLVL_DEBUG) << "End:   ToyHDFFileOutput::endJob()\n";
}

void art::ToyHDFFileOutput::write(EventPrincipal& ep)
{
	using RawEvent = artdaq::Fragments;
	using RawEvents = std::vector<RawEvent>;
	using RawEventHandle = art::Handle<RawEvent>;
	using RawEventHandles = std::vector<RawEventHandle>;

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

		if (!raw_event_handle.isValid()) continue;

		for (auto const& fragment : *raw_event_handle)
		{
			auto sequence_id = fragment.sequenceID();
			auto fragid_id = fragment.fragmentID();
			TLOG(TLVL_TRACE) << "ToyHDFFileOutput::write seq=" << sequence_id << " frag=" << fragid_id << " "
							 << reinterpret_cast<const void*>(fragment.headerBeginBytes()) << " bytes=0x" << std::hex
							 << fragment.sizeBytes() << " start";

			if (fragment.type() == demo::FragmentType::TOY1 || fragment.type() == demo::FragmentType::TOY2)
			{
				toy_ntuple_.insert(fragment);
			}

			TLOG(5) << "ToyHDFFileOutput::write seq=" << sequence_id << " frag=" << fragid_id << " done errno=" << errno;
		}
	}
#if ART_HEX_VERSION < 0x30000
	fstats_.recordEvent(ep.id());
#else
	fstats_.recordEvent(ep.eventID());
#endif
	return;
}

DEFINE_ART_MODULE(art::ToyHDFFileOutput)
