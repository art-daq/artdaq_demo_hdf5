#include <memory>

#include "tracemf.h"
#define TRACE_NAME "HighFiveNtupleDataset"

#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-demo-hdf5/HDF5/highFive/highFiveNtupleDataset.hh"

artdaq::hdf5::HighFiveNtupleDataset::HighFiveNtupleDataset(fhicl::ParameterSet const& ps)
    : FragmentDataset(ps, ps.get<std::string>("mode", "write"))
    , file_(nullptr)
    , headerIndex_(0)
    , fragmentIndex_(0)
    , nWordsPerRow_(ps.get<size_t>("nWordsPerRow", 10240))

{
	TLOG(TLVL_DEBUG) << "HighFiveNtupleDataset Constructor BEGIN";
	auto payloadChunkSize = ps.get<size_t>("payloadChunkSize", 128);
	HighFive::DataSetAccessProps payloadAccessProps;
	payloadAccessProps.add(HighFive::Caching(12421, ps.get<size_t>("chunkCacheSizeBytes", sizeof(artdaq::RawDataType) * payloadChunkSize * nWordsPerRow_ * 10), 0.5));

	if (mode_ == FragmentDatasetMode::Read)
	{
		TLOG(TLVL_TRACE) << "HighFiveNtupleDataset: Opening input file and getting Dataset pointers";
		file_ = std::make_unique<HighFive::File>(ps.get<std::string>("fileName"), HighFive::File::ReadOnly);

		auto fragmentGroup = file_->getGroup("/Fragments");
		fragment_datasets_["sequenceID"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.getDataSet("sequenceID"));
		fragment_datasets_["fragmentID"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.getDataSet("fragmentID"));
		fragment_datasets_["timestamp"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.getDataSet("timestamp"));
		fragment_datasets_["type"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.getDataSet("type"));
		fragment_datasets_["size"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.getDataSet("size"));
		fragment_datasets_["index"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.getDataSet("index"));
		fragment_datasets_["payload"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.getDataSet("payload", payloadAccessProps));
		auto headerGroup = file_->getGroup("/EventHeaders");
		event_datasets_["run_id"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.getDataSet("run_id"));
		event_datasets_["subrun_id"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.getDataSet("subrun_id"));
		event_datasets_["event_id"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.getDataSet("event_id"));
		event_datasets_["sequenceID"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.getDataSet("sequenceID"));
		event_datasets_["timestamp"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.getDataSet("timestamp"));
		event_datasets_["is_complete"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.getDataSet("is_complete"));
	}
	else
	{
		TLOG(TLVL_TRACE) << "HighFiveNtupleDataset: Creating output file";
		file_ = std::make_unique<HighFive::File>(ps.get<std::string>("fileName"), HighFive::File::OpenOrCreate | HighFive::File::Truncate);

		HighFive::DataSetCreateProps scalar_props;
		scalar_props.add(HighFive::Chunking(std::vector<hsize_t>{128, 1}));
		HighFive::DataSetCreateProps vector_props;
		vector_props.add(HighFive::Chunking(std::vector<hsize_t>{payloadChunkSize, nWordsPerRow_}));

		HighFive::DataSpace scalarSpace = HighFive::DataSpace({0, 1}, {HighFive::DataSpace::UNLIMITED, 1});
		HighFive::DataSpace vectorSpace = HighFive::DataSpace({0, nWordsPerRow_}, {HighFive::DataSpace::UNLIMITED, nWordsPerRow_});

		TLOG(TLVL_TRACE) << "HighFiveNtupleDataset: Creating Fragment datasets";
		auto fragmentGroup = file_->createGroup("/Fragments");
		fragment_datasets_["sequenceID"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.createDataSet<uint64_t>("sequenceID", scalarSpace, scalar_props));
		fragment_datasets_["fragmentID"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.createDataSet<uint16_t>("fragmentID", scalarSpace, scalar_props));
		fragment_datasets_["timestamp"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.createDataSet<uint64_t>("timestamp", scalarSpace, scalar_props));
		fragment_datasets_["type"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.createDataSet<uint8_t>("type", scalarSpace, scalar_props));
		fragment_datasets_["size"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.createDataSet<uint64_t>("size", scalarSpace, scalar_props));
		fragment_datasets_["index"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.createDataSet<uint64_t>("index", scalarSpace, scalar_props));
		fragment_datasets_["payload"] = std::make_unique<HighFiveDatasetHelper>(fragmentGroup.createDataSet<artdaq::RawDataType>("payload", vectorSpace, vector_props, payloadAccessProps), payloadChunkSize);

		TLOG(TLVL_TRACE) << "HighFiveNtupleDataset: Creating EventHeader datasets";
		auto headerGroup = file_->createGroup("/EventHeaders");
		event_datasets_["run_id"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.createDataSet<uint32_t>("run_id", scalarSpace, scalar_props));
		event_datasets_["subrun_id"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.createDataSet<uint32_t>("subrun_id", scalarSpace, scalar_props));
		event_datasets_["event_id"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.createDataSet<uint32_t>("event_id", scalarSpace, scalar_props));
		event_datasets_["sequenceID"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.createDataSet<uint64_t>("sequenceID", scalarSpace, scalar_props));
		event_datasets_["timestamp"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.createDataSet<uint64_t>("timestamp", scalarSpace, scalar_props));
		event_datasets_["is_complete"] = std::make_unique<HighFiveDatasetHelper>(headerGroup.createDataSet<uint8_t>("is_complete", scalarSpace, scalar_props));
	}
	TLOG(TLVL_DEBUG) << "HighFiveNtupleDataset Constructor END";
}

artdaq::hdf5::HighFiveNtupleDataset::~HighFiveNtupleDataset() noexcept
{
	TLOG(TLVL_DEBUG) << "~HighFiveNtupleDataset BEGIN/END";
	//	file_->flush();
}

void artdaq::hdf5::HighFiveNtupleDataset::insertOne(artdaq::Fragment const& frag)
{
	TLOG(TLVL_TRACE) << "insertOne BEGIN";
	auto fragSize = frag.size();
	auto rows = static_cast<size_t>(floor(fragSize / static_cast<double>(nWordsPerRow_))) + (fragSize % nWordsPerRow_ == 0 ? 0 : 1);
	TLOG(5) << "Fragment size: " << fragSize << ", rows: " << rows << " (nWordsPerRow: " << nWordsPerRow_ << ")";

	TLOG(6) << "First words of Fragment: 0x" << std::hex << *frag.headerBegin() << " 0x" << std::hex << *(frag.headerBegin() + 1) << " 0x" << std::hex << *(frag.headerBegin() + 2) << " 0x" << std::hex << *(frag.headerBegin() + 3) << " 0x" << std::hex << *(frag.headerBegin() + 4);

	auto seqID = frag.sequenceID();
	auto fragID = frag.fragmentID();
	auto timestamp = frag.timestamp();
	auto type = frag.type();

	for (size_t ii = 0; ii < rows; ++ii)
	{
		TLOG(7) << "Writing Fragment fields to datasets";
		fragment_datasets_["sequenceID"]->write(seqID);
		fragment_datasets_["fragmentID"]->write(fragID);
		fragment_datasets_["timestamp"]->write(timestamp);
		fragment_datasets_["type"]->write(type);
		fragment_datasets_["size"]->write(fragSize);
		fragment_datasets_["index"]->write(ii * nWordsPerRow_);

		auto wordsThisRow = (ii + 1) * nWordsPerRow_ <= fragSize ? nWordsPerRow_ : fragSize - (ii * nWordsPerRow_);
		fragment_datasets_["payload"]->write(frag.headerBegin() + (ii * nWordsPerRow_), wordsThisRow);
	}
	TLOG(TLVL_TRACE) << "insertOne END";
}

void artdaq::hdf5::HighFiveNtupleDataset::insertHeader(artdaq::detail::RawEventHeader const& hdr)
{
	TLOG(TLVL_TRACE) << "insertHeader BEGIN";
	event_datasets_["run_id"]->write(hdr.run_id);
	event_datasets_["subrun_id"]->write(hdr.subrun_id);
	event_datasets_["event_id"]->write(hdr.event_id);
	event_datasets_["sequenceID"]->write(hdr.sequence_id);
	event_datasets_["timestamp"]->write(hdr.timestamp);
	event_datasets_["is_complete"]->write(hdr.is_complete);

	TLOG(TLVL_TRACE) << "insertHeader END";
}

std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> artdaq::hdf5::HighFiveNtupleDataset::readNextEvent()
{
	TLOG(TLVL_TRACE) << "readNextEvent START fragmentIndex_ " << fragmentIndex_;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> output;

	auto numFragments = fragment_datasets_["sequenceID"]->getDatasetSize();
	artdaq::Fragment::sequence_id_t currentSeqID = 0;

	while (fragmentIndex_ < numFragments)
	{
		TLOG(8) << "readNextEvent: Testing Fragment " << fragmentIndex_ << " / " << numFragments << " to see if it belongs in this event";
		if (currentSeqID == 0)
		{
			currentSeqID = fragment_datasets_["sequenceID"]->readOne<uint64_t>(fragmentIndex_);
			TLOG(8) << "readNextEvent: Setting current Sequence ID to " << currentSeqID;
		}

		auto sequence_id = fragment_datasets_["sequenceID"]->readOne<uint64_t>(fragmentIndex_);
		if (sequence_id != currentSeqID)
		{
			TLOG(8) << "readNextEvent: Current sequence ID is " << currentSeqID << ", next Fragment sequence ID is " << sequence_id << ", leaving read loop";
			break;
		}

		auto payloadRowSize = fragment_datasets_["payload"]->getRowSize();

		auto type = fragment_datasets_["type"]->readOne<uint8_t>(fragmentIndex_);
		auto size_words = fragment_datasets_["size"]->readOne<uint64_t>(fragmentIndex_);
		auto index = fragment_datasets_["index"]->readOne<uint64_t>(fragmentIndex_);
		artdaq::Fragment frag(size_words - artdaq::detail::RawFragmentHeader::num_words());

		TLOG(8) << "readNextEvent: Fragment has size " << size_words << ", payloadRowSize is " << payloadRowSize;
		auto thisRowSize = size_words > payloadRowSize ? payloadRowSize : size_words;
		auto payloadvec = fragment_datasets_["payload"]->read<artdaq::RawDataType>(fragmentIndex_);
		memcpy(frag.headerBegin(), &(payloadvec[0]), thisRowSize * sizeof(artdaq::RawDataType));

		TLOG(8) << "readNextEvent: First words of Payload: 0x" << std::hex << payloadvec[0] << " 0x" << std::hex << payloadvec[1] << " 0x" << std::hex << payloadvec[2] << " 0x" << std::hex << payloadvec[3] << " 0x" << std::hex << payloadvec[4];
		TLOG(8) << "readNextEvent: First words of Fragment: 0x" << std::hex << *frag.headerBegin() << " 0x" << std::hex << *(frag.headerBegin() + 1) << " 0x" << std::hex << *(frag.headerBegin() + 2) << " 0x" << std::hex << *(frag.headerBegin() + 3) << " 0x" << std::hex << *(frag.headerBegin() + 4);

		while (index + payloadRowSize < size_words)
		{
			TLOG(8) << "readNextEvent: Retrieving additional payload row, index of previous row " << index << ", payloadRowSize " << payloadRowSize << ", fragment size words " << size_words;
			fragmentIndex_++;
			index = fragment_datasets_["index"]->readOne<size_t>(fragmentIndex_);
			payloadvec = fragment_datasets_["payload"]->read<artdaq::RawDataType>(fragmentIndex_);

			auto thisRowSize = index + payloadRowSize < size_words ? payloadRowSize : size_words - index;
			memcpy(frag.headerBegin() + index, &(payloadvec[0]), thisRowSize * sizeof(artdaq::RawDataType));
		}

		if (output.count(type) == 0u)
		{
			output[type] = std::make_unique<artdaq::Fragments>();
		}
		TLOG(8) << "readNextEvent: Adding Fragment to event map; type=" << type << ", frag size " << frag.size();
		output[type]->push_back(frag);

		fragmentIndex_++;
	}

	TLOG(TLVL_TRACE) << "readNextEvent END output.size() = " << output.size();
	return output;
}

std::unique_ptr<artdaq::detail::RawEventHeader> artdaq::hdf5::HighFiveNtupleDataset::getEventHeader(artdaq::Fragment::sequence_id_t const& seqID)
{
	TLOG(TLVL_TRACE) << "getEventHeader BEGIN";
	artdaq::Fragment::sequence_id_t sequence_id = 0;
	auto numHeaders = event_datasets_["sequenceID"]->getDatasetSize();

	TLOG(9) << "getEventHeader: Searching for matching header row";
	while (sequence_id != seqID && headerIndex_ < numHeaders)
	{
		sequence_id = event_datasets_["sequenceID"]->readOne<uint64_t>(headerIndex_);
		if (sequence_id != seqID)
		{
			headerIndex_++;
		}
	}

	if (headerIndex_ >= numHeaders)
	{
		return nullptr;
	}

	TLOG(9) << "getEventHeader: Matching header found. Populating output";
	auto runID = event_datasets_["run_id"]->readOne<uint32_t>(headerIndex_);
	auto subrunID = event_datasets_["subrun_id"]->readOne<uint32_t>(headerIndex_);
	auto eventID = event_datasets_["event_id"]->readOne<uint32_t>(headerIndex_);
	auto timestamp = event_datasets_["timestamp"]->readOne<uint64_t>(headerIndex_);

	artdaq::detail::RawEventHeader hdr(runID, subrunID, eventID, sequence_id, timestamp);
	hdr.is_complete = (event_datasets_["is_complete"]->readOne<uint8_t>(headerIndex_) != 0u);

	TLOG(TLVL_TRACE) << "getEventHeader END";
	return std::make_unique<artdaq::detail::RawEventHeader>(hdr);
}

DEFINE_ARTDAQ_DATASET_PLUGIN(artdaq::hdf5::HighFiveNtupleDataset)
