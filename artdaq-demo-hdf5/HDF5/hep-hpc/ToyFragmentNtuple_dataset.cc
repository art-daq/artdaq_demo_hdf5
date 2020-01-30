
#include "artdaq-core-demo/Overlays/FragmentType.hh"
#include "artdaq-core-demo/Overlays/ToyFragment.hh"
#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"
#include "artdaq-demo-hdf5/HDF5/hep-hpc/ToyFragmentNtuple.hh"
#include "hep_hpc/hdf5/make_column.hpp"
#include "hep_hpc/hdf5/make_ntuple.hpp"

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

artdaq::hdf5::ToyFragmentNtuple::ToyFragmentNtuple(fhicl::ParameterSet const& ps)

    : FragmentDataset(ps, ps.get<std::string>("mode", "write"))
    , output_(hep_hpc::hdf5::make_ntuple({ps.get<std::string>("fileName", "toyFragments.hdf5"), "TOYFragments"},
                                         hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_scalar_column<uint16_t>("fragmentID", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_scalar_column<uint64_t>("timestamp", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_scalar_column<uint8_t>("type", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_scalar_column<uint16_t>("board_serial_number", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_scalar_column<uint8_t>("num_adc_bits", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_scalar_column<uint32_t>("trigger_number", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_scalar_column<uint8_t>("distribution_type", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_scalar_column<uint32_t>("total_adcs", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_scalar_column<uint32_t>("start_adc", SCALAR_PROPERTIES),
                                         hep_hpc::hdf5::make_column<uint16_t, 1>("ADCs", {nWordsPerRow_}, ARRAY_PROPERTIES)))
    , fragments_(ps, output_.file())
{
	if (mode_ == FragmentDatasetMode::Read)
	{
		TLOG(TLVL_ERROR) << "ToyFragmentDataset configured in read mode but is not capable of reading!";
	}
}

artdaq::hdf5::ToyFragmentNtuple::~ToyFragmentNtuple()
{
	output_.flush();
}

void artdaq::hdf5::ToyFragmentNtuple::insert(artdaq::Fragment const& f)
{
	if (f.type() != demo::FragmentType::TOY1 && f.type() != demo::FragmentType::TOY2)
	{
		fragments_.insert(f);
	}
	else
	{
		demo::ToyFragment frag(f);
		auto m = f.metadata<demo::ToyFragment::Metadata>();
		auto event_size = frag.total_adc_values();
		for (size_t ii = 0; ii < event_size; ii += nWordsPerRow_)
		{
			if (ii + nWordsPerRow_ <= event_size)
			{
				output_.insert(f.sequenceID(), f.fragmentID(), f.timestamp(), f.type(),
				               m->board_serial_number, m->num_adc_bits, frag.hdr_trigger_number(),
				               frag.hdr_distribution_type(), event_size, ii, &frag.dataBeginADCs()[ii]);
			}
			else
			{
				std::vector<demo::ToyFragment::adc_t> adcs(nWordsPerRow_, 0);
				std::copy(frag.dataBeginADCs() + ii, frag.dataEndADCs(), adcs.begin());
				output_.insert(f.sequenceID(), f.fragmentID(), f.timestamp(), f.type(),
				               m->board_serial_number, m->num_adc_bits, frag.hdr_trigger_number(),
				               frag.hdr_distribution_type(), event_size, ii, &adcs[0]);
			}
		}
	}
}

void artdaq::hdf5::ToyFragmentNtuple::insert(artdaq::detail::RawEventHeader const& evtHdr) { fragments_.insert(evtHdr); }

DEFINE_ARTDAQ_DATASET_PLUGIN(artdaq::hdf5::ToyFragmentNtuple)