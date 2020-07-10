#ifndef artdaq_demo_hdf5_HDF5_highFive_highFiveNtupleDataset_hh
#define artdaq_demo_hdf5_HDF5_highFive_highFiveNtupleDataset_hh 1

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "artdaq-demo-hdf5/HDF5/FragmentDataset.hh"

#include "artdaq-demo-hdf5/HDF5/highFive/HighFive/include/highfive/H5File.hpp"
#include "artdaq-demo-hdf5/HDF5/highFive/highFiveDatasetHelper.hh"

#include <unordered_map>

namespace artdaq {
namespace hdf5 {

/**
	 * @brief An implementation of FragmentDataset using the HighFive backend to produce files identical to those produced by the hep_hpc backend (FragmentNtuple)
	 */
class HighFiveNtupleDataset : public FragmentDataset
{
public:
	/**
	 * @brief HighFiveNtupleDataset Constructor
	 * @param ps ParameterSet used to configure HighFiveNtupleDataset
	 *
	 * HighFiveNtupleDataset accepts the following Parameters:
	 * "mode" (Default: "write"): Mode string to use for this FragmentDataset
     * "nWordsPerRow" (Default: 10240): Number of payload words to store in each row of the Dataset
     * "payloadChunkSize" (Default: 128): Size of the payload Ntuple's chunks, in rows
	 * "chunkCacheSizeBytes" (Default: 10 chunks): Size of the chunk cache, in bytes
     * "fileName" (REQUIRED): HDF5 file to read/write
	 */
	HighFiveNtupleDataset(fhicl::ParameterSet const& ps);

	/**
	 * @brief HighFiveNtupleDataset Destructor
	 */
	virtual ~HighFiveNtupleDataset() noexcept;

	/**
	 * @brief Insert a Fragment into the Dataset (write it to the HDF5 file)
	 * @param frag Fragment to insert
	 *
	 * This FragmentDataset plugin uses the "Ntuple" file format, where each Fragment is represented by one or more rows in the Fragments Ntuple Group.
	 */
	void insertOne(Fragment const& frag, std::string instance_name) override;

	/**
	 * @brief Insert a RawEventHeader into the Dataset (write it to the HDF5 file)
	 * @param hdr RawEventHeader to insert
	 *
	 * This FragmentDataset plugin uses the "Ntuple" file format, where each RawEventHeader is represented by a row in the EventHeaders Ntuple Group.
	 */
	void insertHeader(detail::RawEventHeader const& hdr) override;

	/**
	 * @brief Read the next event from the Dataset (HDF5 file)
	 * @returns A Map of Fragment::type_t and pointers to Fragments, suitable for ArtdaqInput
	 */
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() override;

	/**
	 * @brief Read a RawEventHeader from the Dataset (HDF5 file)
	 * @param seqID Sequence ID of the RawEventHeader (should be equivalent to event number)
	 * @return Pointer to a RawEventHeader if a match was found in the Dataset, nullptr otherwise
	 */
	std::unique_ptr<artdaq::detail::RawEventHeader> getEventHeader(artdaq::Fragment::sequence_id_t const&) override;

private:
	std::unique_ptr<HighFive::File> file_;
	size_t headerIndex_;
	size_t fragmentIndex_;
	size_t nWordsPerRow_;

	std::unordered_map<std::string, std::unique_ptr<HighFiveDatasetHelper>> fragment_datasets_;
	std::unordered_map<std::string, std::unique_ptr<HighFiveDatasetHelper>> event_datasets_;
};
}  // namespace hdf5
}  // namespace artdaq
#endif  //artdaq_demo_hdf5_HDF5_highFive_HighFiveNtupleDataset_hh
