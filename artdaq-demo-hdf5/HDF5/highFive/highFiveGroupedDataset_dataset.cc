#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"
#include "artdaq-demo-hdf5/HDF5/highFive/HighFive/include/highfive/H5File.hpp"
#include "artdaq/DAQdata/Globals.hh"
#include "canvas/Persistency/Provenance/EventID.h"
#include <unordered_map>

#define TRACE_NAME "HighFiveGroupedDataset"

namespace artdaq {
namespace hdf5 {
class HighFiveGroupedDataset : public FragmentDataset
{
public:
	HighFiveGroupedDataset(fhicl::ParameterSet const& ps);
	virtual ~HighFiveGroupedDataset();

	void insertOne(artdaq::Fragment const& frag) override;
	void insertMany(artdaq::Fragments const& frags) override;
	void insertHeader(artdaq::detail::RawEventHeader const& hdr) override;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() override;
	std::unique_ptr<artdaq::detail::RawEventHeader> GetEventHeader(artdaq::Fragment::sequence_id_t const& seqID) override;

private:
	std::unique_ptr<HighFive::File> file_;
	size_t headerIndex_;
};
}  // namespace hdf5
}  // namespace artdaq

artdaq::hdf5::HighFiveGroupedDataset::HighFiveGroupedDataset(fhicl::ParameterSet const& ps)
    : FragmentDataset(ps, ps.get<std::string>("mode", "write")), file_(nullptr), headerIndex_(0)
{
	if (mode_ == FragmentDatasetMode::Read)
	{
		file_.reset(new HighFive::File(ps.get<std::string>("fileName"), HighFive::File::ReadOnly));
	}
	else
	{
		file_.reset(new HighFive::File(ps.get<std::string>("fileName"), HighFive::File::OpenOrCreate | HighFive::File::Truncate));
	}
}

artdaq::hdf5::HighFiveGroupedDataset::~HighFiveGroupedDataset()
{
	//	file_->flush();
}

void artdaq::hdf5::HighFiveGroupedDataset::insertOne(artdaq::Fragment const& frag)
{
	if (!file_->exist(std::to_string(frag.sequenceID())))
	{
		file_->createGroup(std::to_string(frag.sequenceID()));
	}
	auto eventGroup = file_->getGroup(std::to_string(frag.sequenceID()));

	if (frag.type() == Fragment::ContainerFragmentType)
	{
		ContainerFragment cf(frag);
		if (eventGroup.exist(std::to_string(cf.fragment_type())))
		{
			eventGroup.createGroup(std::to_string(cf.fragment_type()));
		}
		auto typeGroup = eventGroup.getGroup(std::to_string(cf.fragment_type()));
	}
	else
	{
	}
}

void artdaq::hdf5::HighFiveGroupedDataset::insertMany(artdaq::Fragments const& )
{
}

void artdaq::hdf5::HighFiveGroupedDataset::insertHeader(artdaq::detail::RawEventHeader const& hdr)
{
	if (!file_->exist(std::to_string(hdr.sequence_id)))
	{
		file_->createGroup(std::to_string(hdr.sequence_id));
	}
	auto eventGroup = file_->getGroup(std::to_string(hdr.sequence_id));
	eventGroup.createAttribute("run_id", hdr.run_id);
	eventGroup.createAttribute("subrun_id", hdr.subrun_id);
	eventGroup.createAttribute("event_id", hdr.event_id);
	eventGroup.createAttribute("is_complete", hdr.is_complete);
}

std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> artdaq::hdf5::HighFiveGroupedDataset::readNextEvent()
{
	TLOG(TLVL_DEBUG) << "readNextEvent START";
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> output;

	TLOG(TLVL_DEBUG) << "readNextEvent END output.size() = " << output.size();
	return output;
}

std::unique_ptr<artdaq::detail::RawEventHeader> artdaq::hdf5::HighFiveGroupedDataset::GetEventHeader(artdaq::Fragment::sequence_id_t const& seqID)
{
	if (!file_->exist(std::to_string(seqID)))
	{
		TLOG(TLVL_ERROR) << "Sequence ID " << seqID << " not found in input file!";
		return nullptr;
	}
	auto seqIDGroup = file_->getGroup(std::to_string(seqID));
	
	uint32_t runID, subrunID, eventID;
	seqIDGroup.getAttribute("run_id").read(runID);
	seqIDGroup.getAttribute("subrun_id").read(subrunID);
	seqIDGroup.getAttribute("event_id").read(eventID);

	artdaq::detail::RawEventHeader hdr(runID, subrunID, eventID, seqID);
	seqIDGroup.getAttribute("is_complete").read(hdr.is_complete);

	return std::make_unique<artdaq::detail::RawEventHeader>(hdr);
}

DEFINE_ARTDAQ_DATASET_PLUGIN(artdaq::hdf5::HighFiveGroupedDataset)
