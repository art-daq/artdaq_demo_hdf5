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

	HighFive::DataSetCreateProps fragmentCProps;
	HighFive::DataSetAccessProps fragmentAProps;

	if (frag.type() == Fragment::ContainerFragmentType)
	{
		ContainerFragment cf(frag);
		if (cf.block_count() > 0)
		{
			auto fragPtr = cf.at(0);
			auto typeName = namingService_->GetInstanceNameForFragment(*fragPtr, "unidentified").second;
			if (eventGroup.exist(typeName))
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
				HighFive::DataSpace fragmentSpace = HighFive::DataSpace({fragPtr->size() - fragPtr->headerSizeWords(), 1});
				auto fragDset = containerGroup.createDataSet<RawDataType>("Fragment" + std::to_string(ii), fragmentSpace, fragmentCProps, fragmentAProps);
				fragDset.write(fragPtr->headerBegin() + fragPtr->headerSizeWords());
				fragDset.createAttribute("version", fragPtr->version());
				fragDset.createAttribute("type", fragPtr->type());
				fragDset.createAttribute("sequence_id", fragPtr->sequenceID());
				fragDset.createAttribute("fragment_id", fragPtr->fragmentID());
				fragDset.createAttribute("timestamp", fragPtr->timestamp());
			}
		}
		else
		{
			auto typeName = namingService_->GetInstanceNameForFragment(frag, "unidentified").second;
			if (eventGroup.exist(typeName))
			{
				eventGroup.createGroup(typeName);
			}
			auto typeGroup = eventGroup.getGroup(typeName);

			HighFive::DataSpace fragmentSpace = HighFive::DataSpace({frag.size() - frag.headerSizeWords(), 1});

			auto datasetNameBase = "Fragment" + std::to_string(frag.fragmentID());
			auto datasetName = datasetNameBase;
			int counter = 1;
			while (typeGroup.exist(datasetName))
			{
				TLOG(TLVL_WARNING) << "Duplicate Fragment IDs detected! Check your configuration! counter=" << counter;
				datasetName = datasetNameBase + "_" + std::to_string(counter);
				counter++;
			}

			auto fragDset = typeGroup.createDataSet<RawDataType>(datasetName, fragmentSpace, fragmentCProps, fragmentAProps);
			fragDset.write(frag.headerBegin() + frag.headerSizeWords());
			fragDset.createAttribute("version", frag.version());
			fragDset.createAttribute("type", frag.type());
			fragDset.createAttribute("sequence_id", frag.sequenceID());
			fragDset.createAttribute("fragment_id", frag.fragmentID());
			fragDset.createAttribute("timestamp", frag.timestamp());
		}
	}
	else
	{
		auto typeName = namingService_->GetInstanceNameForFragment(frag, "unidentified").second;
		if (eventGroup.exist(typeName))
		{
			eventGroup.createGroup(typeName);
		}
		auto typeGroup = eventGroup.getGroup(typeName);

		HighFive::DataSpace fragmentSpace = HighFive::DataSpace({frag.size() - frag.headerSizeWords(), 1});

		auto datasetNameBase = "Fragment" + std::to_string(frag.fragmentID());
		auto datasetName = datasetNameBase;
		int counter = 1;
		while (typeGroup.exist(datasetName))
		{
			TLOG(TLVL_WARNING) << "Duplicate Fragment IDs detected! Check your configuration! counter=" << counter;
			datasetName = datasetNameBase + "_" + std::to_string(counter);
			counter++;
		}

		auto fragDset = typeGroup.createDataSet<RawDataType>(datasetName, fragmentSpace, fragmentCProps, fragmentAProps);
		fragDset.write(frag.headerBegin() + frag.headerSizeWords());
		fragDset.createAttribute("version", frag.version());
		fragDset.createAttribute("type", frag.type());
		fragDset.createAttribute("sequence_id", frag.sequenceID());
		fragDset.createAttribute("fragment_id", frag.fragmentID());
		fragDset.createAttribute("timestamp", frag.timestamp());
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

DEFINE_ARTDAQ_DATASET_PLUGIN(artdaq::hdf5::HighFiveGroupedDataset)
