#ifndef artdaq_demo_hdf5_ArtModules_detail_FragmentNtuple
#define artdaq_demo_hdf5_ArtModules_detail_FragmentNtuple 1

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-braces"
#include "hep_hpc/hdf5/Column.hpp"
#include "hep_hpc/hdf5/Ntuple.hpp"
#pragma GCC diagnostic pop

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

namespace artdaq {
namespace hdf5 {

typedef hep_hpc::hdf5::Column<uint8_t, 1> uint8Column;
typedef hep_hpc::hdf5::Column<uint16_t, 1> uint16Column;
typedef hep_hpc::hdf5::Column<uint32_t, 1> uint32Column;
typedef hep_hpc::hdf5::Column<uint64_t, 1> uint64Column;
typedef hep_hpc::hdf5::Column<artdaq::RawDataType, 1> dataColumn;

/**
 * @brief Implemementation of FragmentDataset using hep_hpc Ntuples
 *
 * This implementation is for generic Fragments, a specific implementation for ToyFragments is at ToyFragmentNtuple
 */
class FragmentNtuple : public FragmentDataset
{
public:
	/**
	 * @brief FragmentNtuple Constructor with input hep_hpc::hdf5::File
	 * @param ps ParameterSet for this plugin
	 * @param file File to use for output instead of creating a new one
	 *
	 * FragmentNtuple accepts the following Parameters:
	 * "mode" (Default: "write"): Mode string to use for this FragmentDataset
     * "nWordsPerRow" (Default: 10240): Number of Fragment words to store in each row of the Ntuple
	 */
	FragmentNtuple(fhicl::ParameterSet const& ps, hep_hpc::hdf5::File const& file);

	/**
	 * @brief FragmentNtuple Constructor, creating a new file
	 * @param ps ParameterSet for this plugin
	 *
	 * FragmentNtuple accepts the following Parameters:
	 * "fileName" (Default: "fragments.hdf5"): File name to use
	 * "mode" (Default: "write"): Mode string to use for this FragmentDataset (only "write" mode is supported)
     * "nWordsPerRow" (Default: 10240): Number of Fragment words to store in each row of the Ntuple
	 */
	FragmentNtuple(fhicl::ParameterSet const& ps);

	/**
	 * @brief FragmentNtuple Destructor
	 */
	virtual ~FragmentNtuple();

	/**
	 * @brief Insert a Fragment into the Fragment Ntuple Dataset (write it to the HDF5 file)
	 * @param frag Fragment to insert
	 */
	void insertOne(artdaq::Fragment const& frag) override;

	/**
	 * @brief Insert a RawEventHeader into the Event Header Ntuple Dataset (write it to the HDF5 file)
	 * @param hdr RawEventHeader to insert
	 */
	void insertHeader(artdaq::detail::RawEventHeader const& hdr) override;

	/**
	 * @brief Read the next event from the Dataset (HDF5 file)
	 * @returns A Map of Fragment::type_t and pointers to Fragments, suitable for ArtdaqInput
	 *
	 * This function is not valid for this dataset plugin; it will log an error message and return an empty map
	 */
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() override
	{
		TLOG(TLVL_ERROR) << "FragmentNtuple is not capable of reading!";
		return std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>>();
	}

	/**
	 * @brief Read a RawEventHeader from the Dataset (HDF5 file)
	 * @return Pointer to a RawEventHeader if a match was found in the Dataset, nullptr otherwise
	 *
	 * This function is not valid for this dataset plugin; it will log an error message and return nullptr
	 */
	std::unique_ptr<artdaq::detail::RawEventHeader> getEventHeader(artdaq::Fragment::sequence_id_t const&) override
	{
		TLOG(TLVL_ERROR) << "FragmentNtuple is not capable of reading!";
		return nullptr;
	}

private:
	size_t nWordsPerRow_;
	hep_hpc::hdf5::Ntuple<uint64Column, uint16Column, uint64Column, uint8Column, uint64Column, uint64Column, dataColumn> fragments_;
	hep_hpc::hdf5::Ntuple<uint32Column, uint32Column, uint32Column, uint64Column, uint8Column> eventHeaders_;
};
}  // namespace hdf5
}  // namespace artdaq
#endif  //artdaq_demo_hdf5_ArtModules_detail_FragmentNtuple
