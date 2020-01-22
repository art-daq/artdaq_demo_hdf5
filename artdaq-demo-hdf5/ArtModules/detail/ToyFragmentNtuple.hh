#ifndef artdaq_demo_hdf5_ArtModules_detail_ToyFragmentNtuple
#define artdaq_demo_hdf5_ArtModules_detail_ToyFragmentNtuple 1

#include "hep_hpc/hdf5/make_ntuple.hpp"
#include "hep_hpc/hdf5/make_column.hpp"

#include "artdaq-core-demo/Overlays/ToyFragment.hh"
#include "artdaq-core-demo/Overlays/FragmentType.hh"

#define DO_COMPRESSION 0
#if DO_COMPRESSION
#define SCALAR_PROPERTIES {hep_hpc::hdf5::PropertyList{H5P_DATASET_CREATE}(&H5Pset_deflate, 7u)}
#define ARRAY_PROPERTIES {hep_hpc::hdf5::PropertyList{H5P_DATASET_CREATE}(&H5Pset_deflate, 7u)}
#else
#define SCALAR_PROPERTIES {}
#define ARRAY_PROPERTIES {}
#endif

class ToyFragmentNtuple
{
public:
	ToyFragmentNtuple(std::string fileName, size_t nAdcsPerRow = 512)

		: nAdcsPerRow_(nAdcsPerRow)
		, output_(hep_hpc::hdf5::make_ntuple({fileName, "TOYFragments"},
											 hep_hpc::hdf5::make_scalar_column<uint64_t>("sequenceID", SCALAR_PROPERTIES),
											 hep_hpc::hdf5::make_scalar_column<uint16_t>("fragmentID", SCALAR_PROPERTIES),
											 hep_hpc::hdf5::make_scalar_column<uint64_t>("timestamp", SCALAR_PROPERTIES),
											 hep_hpc::hdf5::make_scalar_column<uint8_t>("type", SCALAR_PROPERTIES),
											 hep_hpc::hdf5::make_scalar_column<uint16_t>("board_serial_number", SCALAR_PROPERTIES),
											 hep_hpc::hdf5::make_scalar_column<uint8_t>("num_adc_bits", SCALAR_PROPERTIES),
											 hep_hpc::hdf5::make_scalar_column<uint32_t>("trigger_number", SCALAR_PROPERTIES),
											 hep_hpc::hdf5::make_scalar_column<uint8_t>("distribution_type", SCALAR_PROPERTIES),
											 hep_hpc::hdf5::make_scalar_column<uint32_t>("total_adcs", SCALAR_PROPERTIES),
											 hep_hpc::hdf5::make_scalar_column<uint32_t>("start_adc",SCALAR_PROPERTIES ),
											 hep_hpc::hdf5::make_column<uint16_t, 1>("ADCs", {nAdcsPerRow_},ARRAY_PROPERTIES )))
	{}

	void insert(artdaq::Fragment const& f)
	{
		demo::ToyFragment frag(f);
		auto m = f.metadata<demo::ToyFragment::Metadata>();
		auto event_size = frag.total_adc_values();
		for (size_t ii = 0; ii < event_size; ii += nAdcsPerRow_)
		{
			if (ii + nAdcsPerRow_ <= event_size)
			{
				output_.insert(f.sequenceID(), f.fragmentID(), f.timestamp(), f.type(),
							   m->board_serial_number, m->num_adc_bits, frag.hdr_trigger_number(),
							   frag.hdr_distribution_type(), event_size, ii, &frag.dataBeginADCs()[ii]);
			}
			else
			{
				std::vector<demo::ToyFragment::adc_t> adcs(nAdcsPerRow_, 0);
				std::copy(frag.dataBeginADCs() + ii, frag.dataEndADCs(), adcs.begin());
				output_.insert(f.sequenceID(), f.fragmentID(), f.timestamp(), f.type(),
							   m->board_serial_number, m->num_adc_bits, frag.hdr_trigger_number(),
							   frag.hdr_distribution_type(), event_size, ii, &adcs[0]);
			}
		}
	}

private:
	size_t nAdcsPerRow_;
	typedef hep_hpc::hdf5::Column<uint8_t, 1> uint8Column;
	typedef hep_hpc::hdf5::Column<uint16_t, 1> uint16Column;
	typedef hep_hpc::hdf5::Column<uint32_t, 1> uint32Column;
	typedef hep_hpc::hdf5::Column<uint64_t, 1> uint64Column;
	hep_hpc::hdf5::Ntuple<uint64Column, uint16Column, uint64Column, uint8Column, uint16Column, uint8Column, uint32Column, uint8Column, uint32Column, uint32Column, uint16Column> output_;
};

#endif  //artdaq_demo_hdf5_ArtModules_detail_ToyFragmentNtuple
