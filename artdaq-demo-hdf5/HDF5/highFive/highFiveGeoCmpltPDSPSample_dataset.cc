#include "tracemf.h"
#define TRACE_NAME "HighFiveGeoCmpltPDSPSample"
#define TLVL_INSERTONE 6
#define TLVL_INSERTHEADER 7
#define TLVL_WRITEFRAGMENT 8
#define TLVL_WRITEFRAGMENT_V 9
#define TLVL_READNEXTEVENT 10
#define TLVL_READNEXTEVENT_V 11
#define TLVL_READFRAGMENT 12
#define TLVL_READFRAGMENT_V 13
#define TLVL_GETEVENTHEADER 14

#include <unordered_map>
#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"
#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"
#include "artdaq-demo-hdf5/HDF5/highFive/HighFive/include/highfive/H5File.hpp"

namespace artdaq {
namespace hdf5 {
/**
 * @brief Sample ProtoDUNE HDF5 output which writes all data indexed by sequence ID
 */
class HighFiveGeoCmpltPDSPSample : public FragmentDataset
{
public:
	/**
	 * @brief HighFiveGeoCmpltPDSPSample Constructor
	 * @param ps ParameterSet for HighFiveGeoCmpltPDSPSample
	 */
	HighFiveGeoCmpltPDSPSample(fhicl::ParameterSet const& ps);
	/**
	 * @brief HighFiveGeoCmpltPDSPSample Destructor
	 */
	virtual ~HighFiveGeoCmpltPDSPSample();

	/**
	 * @brief Write a Fragment to HDF5
	 * @param frag Fragment to write
	 */
	void insertOne(artdaq::Fragment const& frag) override;
	/**
	 * @brief Write Fragments to HDF5
	 * @param frags Fragments to write
	 */
	void insertMany(artdaq::Fragments const& frags) override;
	/**
	 * @brief Write a RawEventHeader to HDF5
	 * @param hdr Header to write
	 */
	void insertHeader(artdaq::detail::RawEventHeader const& hdr) override;
	/**
	 * @brief Read event data from HDF5
	 * @return Fragment data organized by Fragment type
	 */
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() override;
	/**
	 * @brief Read an Event Header from HDF55
	 * @param seqID Sequence ID to read
	 * @return Pointer to RawEventHeader
	 */
	std::unique_ptr<artdaq::detail::RawEventHeader> getEventHeader(artdaq::Fragment::sequence_id_t const& seqID) override;

private:
	std::unique_ptr<HighFive::File> file_;
	size_t eventIndex_;
	HighFive::DataSetCreateProps fragmentCProps_;
	HighFive::DataSetAccessProps fragmentAProps_;

	void writeFragment_(HighFive::Group& group, artdaq::Fragment const& frag);
	artdaq::FragmentPtr readFragment_(HighFive::DataSet const& dataset);
};
}  // namespace hdf5
}  // namespace artdaq

artdaq::hdf5::HighFiveGeoCmpltPDSPSample::HighFiveGeoCmpltPDSPSample(fhicl::ParameterSet const& ps)
    : FragmentDataset(ps, ps.get<std::string>("mode", "write")), file_(nullptr), eventIndex_(0)
{
	TLOG(TLVL_DEBUG) << "HighFiveGeoCmpltPDSPSample CONSTRUCTOR BEGIN";
	if (mode_ == FragmentDatasetMode::Read)
	{
		file_.reset(new HighFive::File(ps.get<std::string>("fileName"), HighFive::File::ReadOnly));
	}
	else
	{
		file_.reset(new HighFive::File(ps.get<std::string>("fileName"), HighFive::File::OpenOrCreate | HighFive::File::Truncate));
	}
	TLOG(TLVL_DEBUG) << "HighFiveGeoCmpltPDSPSample CONSTRUCTOR END";
}

artdaq::hdf5::HighFiveGeoCmpltPDSPSample::~HighFiveGeoCmpltPDSPSample()
{
	TLOG(TLVL_DEBUG) << "~HighFiveGeoCmpltPDSPSample Begin/End ";
	//	file_->flush();
}

void artdaq::hdf5::HighFiveGeoCmpltPDSPSample::insertOne(artdaq::Fragment const& frag)
{
	TLOG(TLVL_TRACE) << "insertOne BEGIN";
	if (!file_->exist(std::to_string(frag.sequenceID())))
	{
		TLOG(TLVL_INSERTONE) << "insertOne: Creating group for sequence ID " << frag.sequenceID();
		file_->createGroup(std::to_string(frag.sequenceID()));
	}
	auto eventGroup = file_->getGroup(std::to_string(frag.sequenceID()));

	// fragment_type_map: [[1, "MISSED"], [2, "TPC"], [3, "PHOTON"], [4, "TRIGGER"], [5, "TIMING"], [6, "TOY1"], [7, "TOY2"], [8, "FELIX"], [9, "CRT"], [10, "CTB"], [11, "CPUHITS"], [12, "DEVBOARDHITS"], [13, "UNKNOWN"]]

	if (frag.type() == Fragment::ContainerFragmentType)
	{
		TLOG(TLVL_INSERTONE) << "insertOne: Processing ContainerFragment";
		ContainerFragment cf(frag);
		if (cf.fragment_type() != 10 && (cf.block_count() > 1 || cf.fragment_type() == 9))
		{
			TLOG(TLVL_INSERTONE) << "insertOne: Getting Fragment type name";
			auto fragPtr = cf.at(0);
			auto typeName = nameHelper_->GetInstanceNameForFragment(*fragPtr).second;
			if (!eventGroup.exist(typeName))
			{
				TLOG(TLVL_INSERTONE) << "insertOne: Creating group for type " << typeName;
				eventGroup.createGroup(typeName);
			}

			TLOG(TLVL_INSERTONE) << "insertOne: Creating group and setting attributes";
			auto typeGroup = eventGroup.getGroup(typeName);
			std::string containerName = "Container0";

			int counter = 1;
			while (typeGroup.exist(containerName))
			{
				// TLOG(TLVL_WRITEFRAGMENT) << "writeFragment_: Duplicate Fragment ID " << frag.fragmentID() << " detected. If this is a ContainerFragment, this is expected, otherwise check configuration!";
				containerName = "Container" + std::to_string(counter);
				counter++;
			}

			auto containerGroup = typeGroup.createGroup(containerName);
			containerGroup.createAttribute("version", frag.version());
			containerGroup.createAttribute("type", frag.type());
			containerGroup.createAttribute("sequence_id", frag.sequenceID());
			containerGroup.createAttribute("fragment_id", frag.fragmentID());
			containerGroup.createAttribute("timestamp", frag.timestamp());

			containerGroup.createAttribute("container_block_count", cf.block_count());
			containerGroup.createAttribute("container_fragment_type", cf.fragment_type());
			containerGroup.createAttribute("container_version", cf.metadata()->version);
			containerGroup.createAttribute("container_missing_data", cf.missing_data());

			TLOG(TLVL_INSERTONE) << "insertOne: Writing Container contained Fragments";
			for (size_t ii = 0; ii < cf.block_count(); ++ii)
			{
				if (ii != 0)
				{
					fragPtr = cf.at(ii);
				}
				writeFragment_(containerGroup, *fragPtr);
			}
		}
		else if (cf.block_count() == 1 || cf.fragment_type() == 10)
		{
			TLOG(TLVL_INSERTONE) << "insertOne: Getting Fragment type name";
			auto fragPtr = cf.at(0);
			auto typeName = nameHelper_->GetInstanceNameForFragment(*fragPtr).second;
			if (!eventGroup.exist(typeName))
			{
				TLOG(TLVL_INSERTONE) << "insertOne: Creating group for type " << typeName;
				eventGroup.createGroup(typeName);
			}

			TLOG(TLVL_INSERTONE) << "insertOne: Creating type group";
			auto typeGroup = eventGroup.getGroup(typeName);
			for (size_t ii = 0; ii < cf.block_count(); ++ii)
			{
				if (ii != 0)
				{
					fragPtr = cf.at(ii);
				}
				writeFragment_(typeGroup, *fragPtr);
			}
		}
#if 0
		else
		{
			TLOG(TLVL_INSERTONE) << "insertOne: Writing Empty Container Fragment as standard Fragment";
			auto typeName = nameHelper_->GetInstanceNameForFragment(frag).second;
			if (!eventGroup.exist(typeName))
			{
				TLOG(TLVL_INSERTONE) << "insertOne: Creating group for type " << typeName;
				eventGroup.createGroup(typeName);
			}
			auto typeGroup = eventGroup.getGroup(typeName);

			writeFragment_(typeGroup, frag);
		}
#endif
	}
	else if (frag.type() == 5)  // Timing
	{
		TLOG(TLVL_INSERTONE) << "insertOne: Writing Timing Fragment";
		writeFragment_(eventGroup, frag);
	}
	else
	{
		TLOG(TLVL_INSERTONE) << "insertOne: Writing non-Container Fragment";
		auto typeName = nameHelper_->GetInstanceNameForFragment(frag).second;
		if (!eventGroup.exist(typeName))
		{
			TLOG(TLVL_INSERTONE) << "insertOne: Creating group for type " << typeName;
			eventGroup.createGroup(typeName);
		}
		auto typeGroup = eventGroup.getGroup(typeName);

		writeFragment_(typeGroup, frag);
	}
	TLOG(TLVL_TRACE) << "insertOne END";
}

void artdaq::hdf5::HighFiveGeoCmpltPDSPSample::insertMany(artdaq::Fragments const& fs)
{
	TLOG(TLVL_TRACE) << "insertMany BEGIN";
	for (auto& f : fs) insertOne(f);
	TLOG(TLVL_TRACE) << "insertMany END";
}

void artdaq::hdf5::HighFiveGeoCmpltPDSPSample::insertHeader(artdaq::detail::RawEventHeader const& hdr)
{
	TLOG(TLVL_TRACE) << "insertHeader BEGIN";
	if (!file_->exist(std::to_string(hdr.sequence_id)))
	{
		TLOG(TLVL_INSERTHEADER) << "insertHeader: Creating group for event " << hdr.sequence_id;
		file_->createGroup(std::to_string(hdr.sequence_id));
	}
	auto eventGroup = file_->getGroup(std::to_string(hdr.sequence_id));
	eventGroup.createAttribute("run_id", hdr.run_id);
	eventGroup.createAttribute("subrun_id", hdr.subrun_id);
	eventGroup.createAttribute("event_id", hdr.event_id);
	eventGroup.createAttribute("is_complete", hdr.is_complete);
	TLOG(TLVL_TRACE) << "insertHeader END";
}

std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> artdaq::hdf5::HighFiveGeoCmpltPDSPSample::readNextEvent()
{
	TLOG(TLVL_DEBUG) << "readNextEvent BEGIN";
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> output;

	TLOG(TLVL_READNEXTEVENT) << "readNextEvent: Finding next event group in file";
	auto groupNames = file_->listObjectNames();
	while (eventIndex_ < groupNames.size() && file_->getObjectType(groupNames[eventIndex_]) != HighFive::ObjectType::Group)
	{
		eventIndex_++;
	}

	if (groupNames.size() <= eventIndex_)
	{
		TLOG(TLVL_INFO) << "readNextEvent: No more events in file!";
	}
	else
	{
		TLOG(TLVL_READNEXTEVENT) << "readNextEvent: Getting event group " << groupNames[eventIndex_];
		auto event_group = file_->getGroup(groupNames[eventIndex_]);
		auto fragment_type_names = event_group.listObjectNames();

		for (auto& fragment_type : fragment_type_names)
		{
			if (event_group.getObjectType(fragment_type) != HighFive::ObjectType::Group)
			{
				continue;
			}
			TLOG(TLVL_READNEXTEVENT) << "readNextEvent: Reading Fragment type " << fragment_type;
			auto type_group = event_group.getGroup(fragment_type);
			auto fragment_names = type_group.listObjectNames();

			for (auto& fragment_name : fragment_names)
			{
				TLOG(TLVL_READNEXTEVENT) << "readNextEvent: Reading Fragment " << fragment_name;
				auto node_type = type_group.getObjectType(fragment_name);
				if (node_type == HighFive::ObjectType::Group)
				{
					TLOG(TLVL_READNEXTEVENT) << "readNextEvent: Fragment " << fragment_name << " is a Container";
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

					TLOG(TLVL_READNEXTEVENT) << "readNextEvent: Creating ContainerFragmentLoader for reading Container Fragments";
					ContainerFragmentLoader cfl(output[type]->back());

					Fragment::type_t container_fragment_type;
					int missing_data;
					container_group.getAttribute("container_fragment_type").read(container_fragment_type);
					container_group.getAttribute("container_missing_data").read(missing_data);

					cfl.set_fragment_type(container_fragment_type);
					cfl.set_missing_data(missing_data);

					TLOG(TLVL_READNEXTEVENT) << "readNextEvent: Reading ContainerFragment Fragments";
					auto fragments = container_group.listObjectNames();
					for (auto& fragname : fragments)
					{
						if (container_group.getObjectType(fragname) != HighFive::ObjectType::Dataset) continue;
						TLOG(TLVL_READNEXTEVENT_V) << "readNextEvent: Calling readFragment_ BEGIN";
						auto frag = readFragment_(container_group.getDataSet(fragname, fragmentAProps_));
						TLOG(TLVL_READNEXTEVENT_V) << "readNextEvent: Calling readFragment_ END";

						TLOG(TLVL_READNEXTEVENT_V) << "readNextEvent: Calling addFragment BEGIN";
						cfl.addFragment(frag);
						TLOG(TLVL_READNEXTEVENT_V) << "readNextEvent: addFragment END";
					}
				}
				else if (node_type == HighFive::ObjectType::Dataset)
				{
					TLOG(TLVL_READNEXTEVENT_V) << "readNextEvent: Calling readFragment_ BEGIN";
					auto frag = readFragment_(type_group.getDataSet(fragment_name, fragmentAProps_));
					TLOG(TLVL_READNEXTEVENT_V) << "readNextEvent: Calling readFragment_ END";

					TLOG(TLVL_READNEXTEVENT) << "readNextEvent: Adding Fragment to output";
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

std::unique_ptr<artdaq::detail::RawEventHeader> artdaq::hdf5::HighFiveGeoCmpltPDSPSample::getEventHeader(artdaq::Fragment::sequence_id_t const& seqID)
{
	TLOG(TLVL_TRACE) << "GetEventHeader BEGIN seqID=" << seqID;
	if (!file_->exist(std::to_string(seqID)))
	{
		TLOG(TLVL_ERROR) << "Sequence ID " << seqID << " not found in input file!";
		return nullptr;
	}
	auto seqIDGroup = file_->getGroup(std::to_string(seqID));

	uint32_t runID, subrunID, eventID;
	uint64_t timestamp;
	seqIDGroup.getAttribute("run_id").read(runID);
	seqIDGroup.getAttribute("subrun_id").read(subrunID);
	seqIDGroup.getAttribute("event_id").read(eventID);
	seqIDGroup.getAttribute("timestamp").read(timestamp);

	TLOG(TLVL_GETEVENTHEADER) << "Creating EventHeader with runID " << runID << ", subrunID " << subrunID << ", eventID " << eventID << ", timestamp " << timestamp << " (seqID " << seqID << ")";
	artdaq::detail::RawEventHeader hdr(runID, subrunID, eventID, seqID, timestamp);
	seqIDGroup.getAttribute("is_complete").read(hdr.is_complete);

	TLOG(TLVL_TRACE) << "GetEventHeader END";
	return std::make_unique<artdaq::detail::RawEventHeader>(hdr);
}

void artdaq::hdf5::HighFiveGeoCmpltPDSPSample::writeFragment_(HighFive::Group& group, artdaq::Fragment const& frag)
{
	TLOG(TLVL_TRACE) << "writeFragment_ BEGIN";

	std::string datasetNameBase = "TimeSlice";
	std::string datasetName = "TimeSlice0";

	switch (frag.type())
	{
		case 2:  // TPC
			datasetNameBase = "APA3." + std::to_string((int)(frag.fragmentID() % 10));
			datasetName = datasetNameBase;
			break;
		case 3:  // Photon
			datasetNameBase = "APA" + std::to_string((int)(frag.fragmentID() / 10)) + "." + std::to_string(-1 + (int)(frag.fragmentID() % 10));
			datasetName = datasetNameBase;
			break;
		case 5:  // Timing
			datasetNameBase = "Timing";
			datasetName = datasetNameBase;
			break;
		case 8:  // FELIX
			int apaNumber = ((int)(frag.fragmentID() / 10)) % 10;
			if (apaNumber == 3) { apaNumber = 2; }
			datasetNameBase = "APA" + std::to_string(apaNumber) + "." + std::to_string((int)(frag.fragmentID() % 10));
			datasetName = datasetNameBase;
			// TLOG(TLVL_DEBUG) << "Skipping Dataset: " << datasetName << ", fragment size=" << frag.size();

			const uint8_t* tmpBeginPtr = frag.dataBeginBytes();
			const uint8_t* tmpEndPtr = frag.dataEndBytes();
			const uint64_t* beginPtr = reinterpret_cast<const uint64_t*>(tmpBeginPtr);
			const uint64_t* endPtr = reinterpret_cast<const uint64_t*>(tmpEndPtr);
			TLOG(TLVL_DEBUG) << "Data addresses in hex: " << std::hex << tmpBeginPtr << ", " << tmpEndPtr << ", " << beginPtr << ", " << endPtr << std::dec;

			uint64_t* mdPtr = reinterpret_cast<uint64_t*>(const_cast<uint8_t*>(frag.headerBeginBytes()) + frag.headerSizeBytes());
			TLOG(TLVL_DEBUG) << "Metadata address and data: " << std::hex << mdPtr << ", " << *mdPtr << std::dec;
			++mdPtr;
			TLOG(TLVL_DEBUG) << "Metadata address and data: " << std::hex << mdPtr << ", " << *mdPtr << std::dec;

			if (frag.size() == 349397)
			{
				uint64_t* dataPtr = const_cast<uint64_t*>(beginPtr);
				++dataPtr;
				for (int idx = 0; idx < 20; ++idx)
				{
					TLOG(TLVL_DEBUG) << std::hex << *dataPtr << std::dec;

					double duneTime = (*dataPtr) * 0.000000020;
					timespec tsp;
					tsp.tv_sec = (time_t)duneTime;
					tsp.tv_nsec = (long)((duneTime - tsp.tv_sec) * 1000000000.0);
					std::string timeString = artdaq::TimeUtils::convertUnixTimeToString(tsp);
					TLOG(TLVL_DEBUG) << "Frame time is " << timeString;

					dataPtr += 58;
				}
				dataPtr += 347072;
				while (dataPtr <= endPtr)
				{
					TLOG(TLVL_DEBUG) << std::hex << *dataPtr << std::dec;

					double duneTime = (*dataPtr) * 0.000000020;
					timespec tsp;
					tsp.tv_sec = (time_t)duneTime;
					tsp.tv_nsec = (long)((duneTime - tsp.tv_sec) * 1000000000.0);
					std::string timeString = artdaq::TimeUtils::convertUnixTimeToString(tsp);
					TLOG(TLVL_DEBUG) << "Frame time is " << timeString;

					dataPtr += 58;
				}
			}
			else
			{
				TLOG(TLVL_DEBUG) << "Skipping Dataset: " << datasetName << ", fragment size=" << frag.size();
			}

			break;
	}

	int counter = 1;
	while (group.exist(datasetName))
	{
		// TLOG(TLVL_WRITEFRAGMENT) << "writeFragment_: Duplicate Fragment ID " << frag.fragmentID() << " detected. If this is a ContainerFragment, this is expected, otherwise check configuration!";
		datasetName = datasetNameBase + std::to_string(counter);
		counter++;
	}

	TLOG(TLVL_WRITEFRAGMENT) << "writeFragment_: Creating DataSpace";
	HighFive::DataSpace fragmentSpace = HighFive::DataSpace({frag.size() - frag.headerSizeWords(), 1});
	auto fragDset = group.createDataSet<RawDataType>(datasetName, fragmentSpace, fragmentCProps_, fragmentAProps_);

	TLOG(TLVL_WRITEFRAGMENT) << "writeFragment_: Creating Attributes from Fragment Header";
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
	// fragDset.createAttribute("atime_ns", fragHdr.atime_ns);
	// fragDset.createAttribute("atime_s", fragHdr.atime_s);

	double duneTime = fragHdr.timestamp * 0.000000020;
	timespec tsp;
	tsp.tv_sec = (time_t)duneTime;
	tsp.tv_nsec = (long)((duneTime - tsp.tv_sec) * 1000000000.0);
	std::string timeString = artdaq::TimeUtils::convertUnixTimeToString(tsp);
	TLOG(TLVL_DEBUG) << "Trigger time is " << timeString << " for Dataset: " << datasetName;
	fragDset.createAttribute("time_string", timeString);

	TLOG(TLVL_WRITEFRAGMENT_V) << "writeFragment_: Writing Fragment payload START";
	fragDset.write(frag.headerBegin() + frag.headerSizeWords());
	TLOG(TLVL_WRITEFRAGMENT_V) << "writeFragment_: Writing Fragment payload DONE";
	TLOG(TLVL_TRACE) << "writeFragment_ END";
}

artdaq::FragmentPtr artdaq::hdf5::HighFiveGeoCmpltPDSPSample::readFragment_(HighFive::DataSet const& dataset)
{
	TLOG(TLVL_TRACE) << "readFragment_ BEGIN";
	size_t fragSize;
	dataset.getAttribute("fragment_data_size").read(fragSize);
	TLOG(TLVL_READFRAGMENT) << "readFragment_: Fragment size " << fragSize << ", dataset size " << dataset.getDimensions()[0];

	artdaq::FragmentPtr frag(new Fragment(fragSize));

	artdaq::Fragment::type_t type;
	size_t metadata_size;
	artdaq::Fragment::sequence_id_t seqID;
	artdaq::Fragment::fragment_id_t fragID;
	artdaq::Fragment::timestamp_t timestamp;
	int valid, complete, atime_ns, atime_s;

	TLOG(TLVL_READFRAGMENT) << "readFragment_: Reading Fragment header fields from dataset attributes";
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

	TLOG(TLVL_READFRAGMENT) << "readFragment_: Copying header into Fragment";
	memcpy(frag->headerAddress(), &fragHdr, sizeof(fragHdr));

	TLOG(TLVL_READFRAGMENT_V) << "readFragment_: Reading payload data into Fragment BEGIN";
	dataset.read(frag->headerAddress() + frag->headerSizeWords());
	TLOG(TLVL_READFRAGMENT_V) << "readFragment_: Reading payload data into Fragment END";

	TLOG(TLVL_TRACE) << "readFragment_ END";
	return frag;
}

DEFINE_ARTDAQ_DATASET_PLUGIN(artdaq::hdf5::HighFiveGeoCmpltPDSPSample)
