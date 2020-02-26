#include <unordered_map>
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"
#include "artdaq-demo-hdf5/HDF5/highFive/HighFive/include/highfive/H5File.hpp"
#include "artdaq/ArtModules/ArtdaqFragmentNamingService.h"
#include "artdaq/DAQdata/Globals.hh"
#include "canvas/Persistency/Provenance/EventID.h"

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
	art::ServiceHandle<ArtdaqFragmentNamingServiceInterface> namingService_;
	HighFive::DataSetCreateProps fragmentCProps_;
	HighFive::DataSetAccessProps fragmentAProps_;

	void writeFragment_(HighFive::Group& group, artdaq::Fragment const& frag);
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
		if (cf.block_count() > 0)
		{
			auto fragPtr = cf.at(0);
			auto typeName = namingService_->GetInstanceNameForFragment(*fragPtr, "unidentified").second;
			if (!eventGroup.exist(typeName))
			{
				eventGroup.createGroup(typeName);
			}
			auto typeGroup = eventGroup.getGroup(typeName);
			auto containerGroup = typeGroup.createGroup("Container_" + std::to_string(frag.fragmentID()));
			containerGroup.createAttribute("version", frag.version());
			containerGroup.createAttribute("type", frag.type());
			containerGroup.createAttribute("sequence_id", frag.sequenceID());
			containerGroup.createAttribute("fragment_id", frag.fragmentID());
			containerGroup.createAttribute("timestamp", frag.timestamp());

			containerGroup.createAttribute("container_block_count", cf.block_count());
			containerGroup.createAttribute("container_fragment_type", cf.fragment_type());
			containerGroup.createAttribute("container_version", cf.metadata()->version);
			containerGroup.createAttribute("container_missing_data", cf.missing_data());

			for (size_t ii = 0; ii < cf.block_count(); ++ii)
			{
				if (ii != 0)
				{
					fragPtr = cf.at(ii);
				}
				writeFragment_(containerGroup, *fragPtr);
			}
		}
		else
		{
			auto typeName = namingService_->GetInstanceNameForFragment(frag, "unidentified").second;
			if (!eventGroup.exist(typeName))
			{
				eventGroup.createGroup(typeName);
			}
			auto typeGroup = eventGroup.getGroup(typeName);

			writeFragment_(typeGroup, frag);
		}
	}
	else
	{
		auto typeName = namingService_->GetInstanceNameForFragment(frag, "unidentified").second;
		if (!eventGroup.exist(typeName))
		{
			eventGroup.createGroup(typeName);
		}
		auto typeGroup = eventGroup.getGroup(typeName);

		writeFragment_(typeGroup, frag);
	}
}

void artdaq::hdf5::HighFiveGroupedDataset::insertMany(artdaq::Fragments const& fs)
{
	for (auto& f : fs) insertOne(f);
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

void artdaq::hdf5::HighFiveGroupedDataset::writeFragment_(HighFive::Group& group, artdaq::Fragment const& frag)
{
	auto datasetNameBase = "Fragment_" + std::to_string(frag.fragmentID());
	auto datasetName = datasetNameBase + ";1";
	int counter = 2;
	while (group.exist(datasetName))
	{
		TLOG(TLVL_DEBUG) << "Duplicate Fragment ID " << frag.fragmentID() << " detected. If this is a ContainerFragment, this is expected, otherwise check configuration!";
		datasetName = datasetNameBase + ";" + std::to_string(counter);
		counter++;
	}

	HighFive::DataSpace fragmentSpace = HighFive::DataSpace({frag.size() - frag.headerSizeWords(), 1});
	auto fragDset = group.createDataSet<RawDataType>(datasetName, fragmentSpace, fragmentCProps_, fragmentAProps_);
	fragDset.write(frag.headerBegin() + frag.headerSizeWords());
	fragDset.createAttribute("version", frag.version());
	fragDset.createAttribute("type", frag.type());
	fragDset.createAttribute("sequence_id", frag.sequenceID());
	fragDset.createAttribute("fragment_id", frag.fragmentID());
	fragDset.createAttribute("timestamp", frag.timestamp());
}

DEFINE_ARTDAQ_DATASET_PLUGIN(artdaq::hdf5::HighFiveGroupedDataset)
