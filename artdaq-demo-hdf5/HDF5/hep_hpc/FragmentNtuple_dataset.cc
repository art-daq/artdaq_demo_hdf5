#include "hep_hpc/hdf5/make_column.hpp"
#include "hep_hpc/hdf5/make_ntuple.hpp"

#include "artdaq-demo-hdf5/HDF5/hep_hpc/FragmentNtuple.hh"

#define DO_COMPRESSION 0
#if DO_COMPRESSION
#define SCALAR_PROPERTIES                                                    \
	{                                                                        \
		hep_hpc::hdf5::PropertyList{H5P_DATASET_CREATE}(&H5Pset_deflate, 7u) \
	}
#define ARRAY_PROPERTIES                                                     \
	{                                                                        \
		hep_hpc::hdf5::PropertyList{H5P_DATASET_CREATE}(&H5Pset_deflate, 7u) \
	}
#else
#define SCALAR_PROPERTIES \
	{}
#define ARRAY_PROPERTIES \
	{}
#endif

artdaq::hdf5::FragmentNtuple::FragmentNtuple(fhicl::ParameterSet const& ps, hep_hpc::hdf5::File const& file)

    : nWordsPerRow_(ps.get<size_t>("nWordsPerRow", 10240))
    , fragments_(hep_hpc::hdf5::make_ntuple({file, "Fragments"},
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint16_t>("fragmentID", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("timestamp", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint8_t>("type", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("size", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("index", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_column<artdaq::RawDataType, 1>("payload", {nWordsPerRow_}, ARRAY_PROPERTIES)))
    , eventHeaders_(hep_hpc::hdf5::make_ntuple({fragments_.file(), "EventHeaders"},
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("run_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("subrun_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("event_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint8_t>("is_complete", SCALAR_PROPERTIES)))
{}

artdaq::hdf5::FragmentNtuple::FragmentNtuple(fhicl::ParameterSet const& ps)

    : nWordsPerRow_(ps.get<size_t>("nWordsPerRow", 10240))
    , fragments_(hep_hpc::hdf5::make_ntuple({ps.get<std::string>("fileName", "fragments.hdf5"), "Fragments"},
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint16_t>("fragmentID", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("timestamp", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint8_t>("type", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("size", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("index", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_column<artdaq::RawDataType, 1>("payload", {nWordsPerRow_}, ARRAY_PROPERTIES)))
    , eventHeaders_(hep_hpc::hdf5::make_ntuple({fragments_.file(), "EventHeaders"},
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("run_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("subrun_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("event_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint8_t>("is_complete", SCALAR_PROPERTIES)))
{}

void artdaq::hdf5::FragmentNtuple::insert(artdaq::Fragment const& frag)
{
	for (size_t ii = 0; ii < frag.size(); ii += nWordsPerRow_)
	{
		if (ii + nWordsPerRow_ <= frag.size())
		{
			fragments_.insert(frag.sequenceID(), frag.fragmentID(), frag.timestamp(), frag.type(),
			                  frag.size(), ii, &frag.dataBegin()[ii]);
		}
		else
		{
			std::vector<artdaq::RawDataType> words(nWordsPerRow_, 0);
			std::copy(frag.dataBegin() + ii, frag.dataEnd(), words.begin());
			fragments_.insert(frag.sequenceID(), frag.fragmentID(), frag.timestamp(), frag.type(),
			                  frag.size(), ii, &words[0]);
		}
	}
}

void artdaq::hdf5::FragmentNtuple::insert(artdaq::detail::RawEventHeader const& hdr)
{
	eventHeaders_.insert(hdr.run_id, hdr.subrun_id, hdr.event_id, hdr.sequence_id, hdr.is_complete);
}