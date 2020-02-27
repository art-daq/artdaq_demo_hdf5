#include <unordered_map>
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "artdaq-core/Data/ContainerFragmentLoader.hh"
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
	size_t eventIndex_;
	art::ServiceHandle<ArtdaqFragmentNamingServiceInterface> namingService_;
	HighFive::DataSetCreateProps fragmentCProps_;
	HighFive::DataSetAccessProps fragmentAProps_;

	void writeFragment_(HighFive::Group& group, artdaq::Fragment const& frag);
	artdaq::FragmentPtr readFragment_(HighFive::DataSet const& dataset);
};
}  // namespace hdf5
}  // namespace artdaq

artdaq::hdf5::HighFiveGroupedDataset::HighFiveGroupedDataset(fhicl::ParameterSet const& ps)
    : FragmentDataset(ps, ps.get<std::string>("mode", "write")), file_(nullptr), eventIndex_(0)
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

	auto groupNames = file_->listObjectNames();
	while (eventIndex_ < groupNames.size() && file_->getObjectType(groupNames[eventIndex_]) != HighFive::ObjectType::Group)
	{
		eventIndex_++;
	}
	if (groupNames.size() <= eventIndex_)
	{
		TLOG(TLVL_INFO) << "No more events in file!";
	}
	else
	{
		auto event_group = file_->getGroup(groupNames[eventIndex_]);
		auto fragment_type_names = event_group.listObjectNames();

		for (auto& fragment_type : fragment_type_names)
		{
			if (event_group.getObjectType(fragment_type) != HighFive::ObjectType::Group)
			{
				continue;
			}
			auto type_group = event_group.getGroup(fragment_type);
			auto fragment_names = type_group.listObjectNames();

			for (auto& fragment_name : fragment_names)
			{
				auto node_type = type_group.getObjectType(fragment_name);
				if (node_type == HighFive::ObjectType::Group)
				{
					auto container_group = type_group.getGroup(fragment_name);
					Fragment::type_t type;
					container_group.getAttribute("type").read<Fragment::type_t>(type);
					Fragment::sequence_id_t seqID;
					container_group.getAttribute("sequence_id").read(seqID);
					Fragment::timestamp_t timestamp;
					container_group.getAttribute("timestamp").read(timestamp);
					Fragment::fragment_id_t fragID;
					container_group.getAttribute("fragment_id").read(fragID);
					if (!output.count(type))
					{
						output[type].reset(new Fragments());
					}
					output[type]->emplace_back(seqID, fragID);
					output[type]->back().setTimestamp(timestamp);
					output[type]->back().setSystemType(type);
					ContainerFragmentLoader cfl(output[type]->back());

					Fragment::type_t container_fragment_type;
					int missing_data;
					container_group.getAttribute("container_fragment_type").read(container_fragment_type);
					container_group.getAttribute("container_missing_data").read(missing_data);

					cfl.set_fragment_type(container_fragment_type);
					cfl.set_missing_data(missing_data);

					auto fragments = container_group.listObjectNames();
					for (auto& fragname : fragments)
					{
						if (container_group.getObjectType(fragname) != HighFive::ObjectType::Dataset) continue;
						auto frag = readFragment_(container_group.getDataSet(fragname, fragmentAProps_));
						cfl.addFragment(frag);
					}
				}
				else if (node_type == HighFive::ObjectType::Dataset)
				{
					auto frag = readFragment_(type_group.getDataSet(fragment_name, fragmentAProps_));
					if (!output.count(frag->type()))
					{
						output[frag->type()].reset(new artdaq::Fragments());
					}
					output[frag->type()]->push_back(*frag.release());
				}
			}
		}
	}
	++eventIndex_;

	TLOG(TLVL_DEBUG)
	    << "readNextEvent END output.size() = " << output.size();
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

	auto fragHdr = frag.fragmentHeader();
	fragDset.createAttribute("word_count", fragHdr.word_count);
	fragDset.createAttribute("fragment_data_size", frag.size() - frag.headerSizeWords());
	fragDset.createAttribute("version", fragHdr.version);
	fragDset.createAttribute("type", fragHdr.type);
	fragDset.createAttribute("metadata_word_count", fragHdr.metadata_word_count);

	fragDset.createAttribute("sequence_id", fragHdr.sequence_id);
	fragDset.createAttribute("fragment_id", fragHdr.fragment_id);

	fragDset.createAttribute("timestamp", fragHdr.timestamp);

	fragDset.createAttribute("valid", fragHdr.valid);
	fragDset.createAttribute("complete", fragHdr.complete);
	fragDset.createAttribute("atime_ns", fragHdr.atime_ns);
	fragDset.createAttribute("atime_s", fragHdr.atime_s);

	fragDset.write(frag.headerBegin() + frag.headerSizeWords());
}

artdaq::FragmentPtr artdaq::hdf5::HighFiveGroupedDataset::readFragment_(HighFive::DataSet const& dataset)
{
	size_t fragSize;
	dataset.getAttribute("fragment_data_size").read(fragSize);
	TLOG(TLVL_DEBUG) << "Fragment size " << fragSize << ", dataset size " << dataset.getDimensions()[0];

	artdaq::FragmentPtr frag(new Fragment(fragSize));

	artdaq::Fragment::type_t type;
	size_t metadata_size;
	artdaq::Fragment::sequence_id_t seqID;
	artdaq::Fragment::fragment_id_t fragID;
	artdaq::Fragment::timestamp_t timestamp;
	int valid, complete, atime_ns, atime_s;

	dataset.getAttribute("type").read(type);
	dataset.getAttribute("metadata_word_count").read(metadata_size);

	dataset.getAttribute("sequence_id").read(seqID);
	dataset.getAttribute("fragment_id").read(fragID);

	dataset.getAttribute("timestamp").read(timestamp);

	dataset.getAttribute("valid").read(valid);
	dataset.getAttribute("complete").read(complete);
	dataset.getAttribute("atime_ns").read(atime_ns);
	dataset.getAttribute("atime_s").read(atime_s);

	auto fragHdr = frag->fragmentHeader();
	fragHdr.type = type;
	fragHdr.metadata_word_count = metadata_size;

	fragHdr.sequence_id = seqID;
	fragHdr.fragment_id = fragID;

	fragHdr.timestamp = timestamp;

	fragHdr.valid = valid;
	fragHdr.complete = complete;
	fragHdr.atime_ns = atime_ns;
	fragHdr.atime_s = atime_s;
	memcpy(frag->headerAddress(), &fragHdr, sizeof(fragHdr));

	dataset.read(frag->headerAddress() + frag->headerSizeWords());

	return frag;
}

DEFINE_ARTDAQ_DATASET_PLUGIN(artdaq::hdf5::HighFiveGroupedDataset)
