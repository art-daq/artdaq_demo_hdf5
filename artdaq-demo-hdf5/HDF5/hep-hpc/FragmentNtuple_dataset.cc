#include "tracemf.h"
#define TRACE_NAME "FragmentNtuple"

#include "artdaq-demo-hdf5/HDF5/hep-hpc/FragmentNtuple.hh"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-braces"
#include "hep_hpc/hdf5/make_column.hpp"
#include "hep_hpc/hdf5/make_ntuple.hpp"
#pragma GCC diagnostic pop

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

    : FragmentDataset(ps, ps.get<std::string>("mode", "write"))
    , nWordsPerRow_(ps.get<size_t>("nWordsPerRow", 10240))
    , fragments_(hep_hpc::hdf5::make_ntuple({file, "Fragments"},
                                            /** \cond */
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint16_t>("fragmentID", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("timestamp", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint8_t>("type", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("size", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("index", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_column<artdaq::RawDataType, 1>("payload", nWordsPerRow_, ARRAY_PROPERTIES)))
    /** \endcond */
    , eventHeaders_(hep_hpc::hdf5::make_ntuple({fragments_.file(), "EventHeaders"},
                                               /** \cond */
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("run_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("subrun_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("event_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint8_t>("is_complete", SCALAR_PROPERTIES)))
/** \endcond */
{
	TLOG(TLVL_DEBUG) << "FragmentNtuple Constructor (file) START";
	if (mode_ == FragmentDatasetMode::Read)
	{
		TLOG(TLVL_ERROR) << "ToyFragmentDataset configured in read mode but is not capable of reading!";
	}
	TLOG(TLVL_DEBUG) << "FragmentNtuple Constructor (file) END";
}

artdaq::hdf5::FragmentNtuple::FragmentNtuple(fhicl::ParameterSet const& ps)

    : FragmentDataset(ps, ps.get<std::string>("mode", "write"))
    , nWordsPerRow_(ps.get<size_t>("nWordsPerRow", 10240))
    , fragments_(hep_hpc::hdf5::make_ntuple({ps.get<std::string>("fileName", "fragments.hdf5"), "Fragments"},
                                            /** \cond */
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint16_t>("fragmentID", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("timestamp", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint8_t>("type", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("size", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_scalar_column<uint64_t>("index", SCALAR_PROPERTIES),
                                            hep_hpc::hdf5::make_column<artdaq::RawDataType, 1>("payload", nWordsPerRow_, ARRAY_PROPERTIES)))
    /** \endcond */
    , eventHeaders_(hep_hpc::hdf5::make_ntuple({fragments_.file(), "EventHeaders"},
                                               /** \cond */
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("run_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("subrun_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint32_t>("event_id", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
                                               hep_hpc::hdf5::make_scalar_column<uint8_t>("is_complete", SCALAR_PROPERTIES)))
/** \endcond */
{
	TLOG(TLVL_DEBUG) << "FragmentNtuple Constructor START";
	if (mode_ == FragmentDatasetMode::Read)
	{
		TLOG(TLVL_ERROR) << "ToyFragmentDataset configured in read mode but is not capable of reading!";
	}
	TLOG(TLVL_DEBUG) << "FragmentNtuple Constructor END";
}

artdaq::hdf5::FragmentNtuple::~FragmentNtuple()
{
	TLOG(TLVL_DEBUG) << "FragmentNtuple Destructor START";
	fragments_.flush();
	TLOG(TLVL_DEBUG) << "FragmentNtuple Destructor END";
}

void artdaq::hdf5::FragmentNtuple::insertOne(artdaq::Fragment const& frag, std::string /*instance_name*/)
{
	TLOG(TLVL_TRACE) << "FragmentNtuple::insertOne BEGIN";

	for (size_t ii = 0; ii < frag.size(); ii += nWordsPerRow_)
	{
		if (ii + nWordsPerRow_ <= frag.size())
		{
			fragments_.insert(frag.sequenceID(), frag.fragmentID(), frag.timestamp(), frag.type(),
			                  frag.size(), ii, &frag.headerBegin()[ii]);
		}
		else
		{
			std::vector<artdaq::RawDataType> words(nWordsPerRow_, 0);
			std::copy(frag.headerBegin() + ii, frag.dataEnd(), words.begin());
			fragments_.insert(frag.sequenceID(), frag.fragmentID(), frag.timestamp(), frag.type(),
			                  frag.size(), ii, &words[0]);
		}
	}
	TLOG(TLVL_TRACE) << "FragmentNtuple::insertOne END";
}

void artdaq::hdf5::FragmentNtuple::insertHeader(artdaq::detail::RawEventHeader const& hdr)
{
	TLOG(TLVL_TRACE) << "FragmentNtuple::insertHeader: Writing header to eventHeaders_ group";
	eventHeaders_.insert(hdr.run_id, hdr.subrun_id, hdr.event_id, hdr.sequence_id, hdr.is_complete);
}

DEFINE_ARTDAQ_DATASET_PLUGIN(artdaq::hdf5::FragmentNtuple)
