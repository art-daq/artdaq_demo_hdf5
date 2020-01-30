#include "artdaq-demo-hdf5/HDF5/highFive/highFiveDataset.hh"

#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME "HighFiveDataset"

artdaq::hdf5::HighFiveDataset::HighFiveDataset(fhicl::ParameterSet const& ps)
    : FragmentDataset(ps, ps.get<std::string>("mode", "write")), file_(nullptr), headerIndex_(0), fragmentIndex_(0)
{
	if (mode_ == FragmentDatasetMode::Read)
	{
		file_.reset(new HighFive::File(ps.get<std::string>("fileName"), HighFive::File::ReadOnly));
	}
	else
	{
		file_.reset(new HighFive::File(ps.get<std::string>("fileName"), HighFive::File::OpenOrCreate | HighFive::File::Truncate));

		HighFive::DataSetCreateProps scalar_props;
		scalar_props.add(HighFive::Chunking(std::vector<hsize_t>{128, 1}));
		HighFive::DataSetCreateProps vector_props;
		vector_props.add(HighFive::Chunking(std::vector<hsize_t>{ps.get<size_t>("payloadChunkSize", 128), nWordsPerRow_}));

		HighFive::DataSpace scalarSpace = HighFive::DataSpace({0, 1}, {HighFive::DataSpace::UNLIMITED, 1});
		HighFive::DataSpace vectorSpace = HighFive::DataSpace({0, nWordsPerRow_}, {HighFive::DataSpace::UNLIMITED, nWordsPerRow_});

		auto fragmentGroup = file_->createGroup("/Fragments");
		fragmentGroup.createDataSet<uint64_t>("sequenceID", scalarSpace, scalar_props);
		fragmentGroup.createDataSet<uint16_t>("fragmentID", scalarSpace, scalar_props);
		fragmentGroup.createDataSet<uint64_t>("timestamp", scalarSpace, scalar_props);
		fragmentGroup.createDataSet<uint8_t>("type", scalarSpace, scalar_props);
		fragmentGroup.createDataSet<uint64_t>("size", scalarSpace, scalar_props);
		fragmentGroup.createDataSet<uint64_t>("index", scalarSpace, scalar_props);
		fragmentGroup.createDataSet<artdaq::RawDataType>("payload", vectorSpace, vector_props);
		auto headerGroup = file_->createGroup("/EventHeaders");
		headerGroup.createDataSet<uint32_t>("run_id", scalarSpace, scalar_props);
		headerGroup.createDataSet<uint32_t>("subrun_id", scalarSpace, scalar_props);
		headerGroup.createDataSet<uint32_t>("event_id", scalarSpace, scalar_props);
		headerGroup.createDataSet<uint64_t>("sequenceID", scalarSpace, scalar_props);
		headerGroup.createDataSet<uint8_t>("is_complete", scalarSpace, scalar_props);
	}
}

artdaq::hdf5::HighFiveDataset::~HighFiveDataset()
{
	file_->flush();
}

void artdaq::hdf5::HighFiveDataset::insert(artdaq::Fragment const& frag)
{
	auto fragments = file_->getGroup("/Fragments");

	
	auto fragSize = frag.size();
	auto rows = static_cast<size_t>(floor(fragSize / static_cast<double>(nWordsPerRow_))) + (fragSize % nWordsPerRow_ == 0 ? 0 : 1);
	TLOG(TLVL_DEBUG) << "Fragment size: " << fragSize << ", rows: " << rows << " (nWordsPerRow: " << nWordsPerRow_ << ")";

	TLOG(TLVL_TRACE) << "First words of Fragment: 0x" << std::hex << *frag.headerBegin() << " 0x" << std::hex << *(frag.headerBegin() + 1) << " 0x" << std::hex << *(frag.headerBegin() + 2) << " 0x" << std::hex << *(frag.headerBegin() + 3) << " 0x" << std::hex << *(frag.headerBegin() + 4);

	auto seqID = frag.sequenceID();
	auto fragID = frag.fragmentID();
	auto timestamp = frag.timestamp();
	auto type = frag.type();

	                    for (size_t ii = 0; ii < rows; ++ii)
	{
		auto dataset = fragments.getDataSet("sequenceID");
		dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
		dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(seqID);

		dataset = fragments.getDataSet("fragmentID");
		dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
		dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(fragID);

		dataset = fragments.getDataSet("timestamp");
		dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
		dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(timestamp);

		dataset = fragments.getDataSet("type");
		dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
		dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(type);

		dataset = fragments.getDataSet("size");
		dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
		dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(fragSize);

		dataset = fragments.getDataSet("index");
		dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
		dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(ii * nWordsPerRow_);

		auto wordsThisRow = (ii + 1) * nWordsPerRow_ <= fragSize ? nWordsPerRow_ : fragSize - (ii * nWordsPerRow_);
		dataset = fragments.getDataSet("payload");
		dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
		dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, wordsThisRow}).write(frag.headerBegin() + (ii * nWordsPerRow_));
	}
}

void artdaq::hdf5::HighFiveDataset::insert(artdaq::detail::RawEventHeader const& hdr)
{
	auto eventHeaders = file_->getGroup("/EventHeaders");
	auto dataset = eventHeaders.getDataSet("run_id");
	dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
	dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(hdr.run_id);

	dataset = eventHeaders.getDataSet("subrun_id");
	dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
	dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(hdr.subrun_id);

	dataset = eventHeaders.getDataSet("event_id");
	dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
	dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(hdr.event_id);

	dataset = eventHeaders.getDataSet("sequenceID");
	dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
	dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(hdr.sequence_id);

	dataset = eventHeaders.getDataSet("is_complete");
	dataset.resize({dataset.getDimensions()[0] + 1, dataset.getDimensions()[1]});
	dataset.select({dataset.getDimensions()[0] - 1, 0}, {1, 1}).write(hdr.is_complete);
}

std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> artdaq::hdf5::HighFiveDataset::readNextEvent()
{
	TLOG(TLVL_DEBUG) << "readNextEvent START fragmentIndex_ " << fragmentIndex_;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> output;

	auto fragments = file_->getGroup("/Fragments");

	auto numFragments = fragments.getDataSet("sequenceID").getDimensions()[0];
	artdaq::Fragment::sequence_id_t currentSeqID = 0;

	while (fragmentIndex_ < numFragments)
	{
		TLOG(TLVL_DEBUG) << "Testing Fragment " << fragmentIndex_ << " / " << numFragments << " to see if it belongs in this event";
		if (currentSeqID == 0)
		{
			std::vector<uint64_t> seqIDvec;
			fragments.getDataSet("sequenceID").select({fragmentIndex_, 0}, {1, 1}).read(seqIDvec);
			currentSeqID = seqIDvec[0];
			TLOG(TLVL_DEBUG) << "Setting current Sequence ID to " << currentSeqID;
		}

		std::vector<uint64_t> seqIDvec;
		fragments.getDataSet("sequenceID").select({fragmentIndex_, 0}, {1, 1}).read(seqIDvec);
		auto sequence_id = seqIDvec[0];
		if (sequence_id != currentSeqID)
		{
			TLOG(TLVL_DEBUG) << "Current sequence ID is " << currentSeqID << ", next Fragment sequence ID is " << sequence_id << ", leaving read loop";
			break;
		}

		std::vector<uint8_t> typevec;
		std::vector<uint64_t> sizevec;
		std::vector<uint64_t> indexvec;
		std::vector<std::vector<artdaq::RawDataType>> payloadvec;
		auto payloadRowSize = fragments.getDataSet("payload").getDimensions()[1];

		fragments.getDataSet("type").select({fragmentIndex_, 0}, {1, 1}).read(typevec);
		fragments.getDataSet("size").select({fragmentIndex_, 0}, {1, 1}).read(sizevec);
		fragments.getDataSet("index").select({fragmentIndex_, 0}, {1, 1}).read(indexvec);
		fragments.getDataSet("payload").select({fragmentIndex_, 0}, {1, payloadRowSize}).read(payloadvec);

		auto type = typevec[0];
		auto size_words = sizevec[0];
		artdaq::Fragment frag(size_words - artdaq::detail::RawFragmentHeader::num_words());

		TLOG(TLVL_DEBUG) << "Fragment has size " << size_words << ", payloadRowSize is " << payloadRowSize;
		auto thisRowSize = size_words > payloadRowSize ? payloadRowSize : size_words;
		memcpy(frag.headerBegin(), &(payloadvec[0][0]), thisRowSize * sizeof(artdaq::RawDataType));

		TLOG(TLVL_TRACE) << "First words of Payload: 0x" << std::hex << payloadvec[0][0] << " 0x" << std::hex << payloadvec[0][1] << " 0x" << std::hex << payloadvec[0][2] << " 0x" << std::hex << payloadvec[0][3] << " 0x" << std::hex << payloadvec[0][4];
		TLOG(TLVL_TRACE) << "First words of Fragment: 0x" << std::hex << *frag.headerBegin() << " 0x" << std::hex << *(frag.headerBegin() + 1) << " 0x" << std::hex << *(frag.headerBegin() + 2) << " 0x" << std::hex << *(frag.headerBegin() + 3) << " 0x" << std::hex << *(frag.headerBegin() + 4);

		while (indexvec[0] + payloadRowSize < size_words)
		{
			TLOG(TLVL_DEBUG) << "Retrieving additional payload row, index of previous row " << indexvec[0] << ", payloadRowSize " << payloadRowSize << ", fragment size words " << size_words;
			fragmentIndex_++;
			fragments.getDataSet("index").select({fragmentIndex_, 0}, {1, 1}).read(indexvec);
			fragments.getDataSet("payload").select({fragmentIndex_, 0}, {1, payloadRowSize}).read(payloadvec);

			auto thisRowSize = indexvec[0] + payloadRowSize < size_words ? payloadRowSize : size_words - indexvec[0];
			memcpy(frag.headerBegin() + indexvec[0], &payloadvec[0][0], thisRowSize * sizeof(artdaq::RawDataType));
		}

		if (!output.count(type))
		{
			output[type] = std::make_unique<artdaq::Fragments>();
		}
		TLOG(TLVL_DEBUG) << "Adding Fragment to event map; type=" << type << ", frag size " << frag.size();
		output[type]->push_back(frag);

		fragmentIndex_++;
	}

	TLOG(TLVL_DEBUG) << "readNextEvent END output.size() = " << output.size();
	return output;
}

std::unique_ptr<artdaq::detail::RawEventHeader> artdaq::hdf5::HighFiveDataset::GetEventHeader(artdaq::Fragment::sequence_id_t const& seqID)
{
	auto eventHeaders = file_->getGroup("/EventHeaders");
	artdaq::Fragment::sequence_id_t sequence_id = 0;
	auto numHeaders = eventHeaders.getDataSet("sequenceID").getDimensions()[0];

	while (sequence_id != seqID && headerIndex_ < numHeaders)
	{
		std::vector<uint64_t> seqIDvec;
		eventHeaders.getDataSet("sequenceID").select({headerIndex_, 0}, {1, 1}).read(seqIDvec);
		sequence_id = seqIDvec[0];
		if (sequence_id != seqID)
		{
			headerIndex_++;
		}
	}

	if (headerIndex_ >= numHeaders) return nullptr;

	std::vector<uint32_t> runIDvec, subrunIDvec, eventIDvec;
	std::vector<uint8_t> isCompletevec;

	eventHeaders.getDataSet("run_id").select({headerIndex_, 0}, {1, 1}).read(runIDvec);
	eventHeaders.getDataSet("subrun_id").select({headerIndex_, 0}, {1, 1}).read(subrunIDvec);
	eventHeaders.getDataSet("event_id").select({headerIndex_, 0}, {1, 1}).read(eventIDvec);
	eventHeaders.getDataSet("is_complete").select({headerIndex_, 0}, {1, 1}).read(isCompletevec);

	artdaq::detail::RawEventHeader hdr(runIDvec[0], subrunIDvec[0], eventIDvec[0], sequence_id);
	hdr.is_complete = isCompletevec[0];

	return std::make_unique<artdaq::detail::RawEventHeader>(hdr);
}

DEFINE_ARTDAQ_DATASET_PLUGIN(artdaq::hdf5::HighFiveDataset)
