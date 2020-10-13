#ifndef artdaq_demo_hdf5_hdf5_FragmentDataset_hh
#define artdaq_demo_hdf5_hdf5_FragmentDataset_hh 1

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/FragmentNameHelper.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "cetlib/compiler_macros.h"
#include "fhiclcpp/ParameterSet.h"

#include <unordered_map>

namespace artdaq {
namespace hdf5 {

/**
	 * @brief Tag for whether this FragmentDataset is for reading or for writing
	 */
enum class FragmentDatasetMode : uint8_t
{
	Read = 0,  ///< This FragmentDataset is reading from a file (not supported by hep_hpc backend)
	Write = 1  ///< This FragmentDataset is writing to a file
};

/**
 * @brief Base class that defines methods for reading and writing to HDF5 files via various implementation plugins
 *
 * This design was chosen so that multiple different HDF5 backends could be evaluated in parallel, as well as different
 * file formats within each backend.
 */
class FragmentDataset
{
public:
	/**
	 * @brief FragmentDataset Constructor
	 * @param ps ParameterSet containing configuration for this FragmentDataset
	 * @param mode String which will be converted to a FragmentDatasetMode. Currently, if this parameter contains "rite", then FragmentDatasetMode::Write will be used, FragmentDatasetMode::Read otherwise.
	 *
	 * FragmentDataset takes no Parameters.
	 */
	FragmentDataset(fhicl::ParameterSet const& ps, const std::string& mode);
	/**
	 * @brief FragmentDataset default virtual destructor
	 */
	virtual ~FragmentDataset() noexcept = default;
	/**
	 * @brief Insert a Fragment into the Dataset (write it to the HDF5 file)
	 * @param f Fragment to insert
	 *
	 * This function is pure virtual.
	 */
	virtual void insertOne(Fragment const& f) = 0;
	/**
	 * @brief Insert several Fragments into the Dataset (write them to the HDF5 file)
	 * @param fs Fragments to insert
	 */
	virtual void insertMany(Fragments const& fs)
	{
		for (auto const& f : fs) insertOne(f);
	}
	/**
	 * @brief Insert a RawEventHeader into the Dataset (write it to the HDF5 file)
	 * @param e RawEventHeader to insert
	 *
	 * This function is pure virtual.
	 */
	virtual void insertHeader(detail::RawEventHeader const& e) = 0;
	/**
	 * @brief Read the next event from the Dataset (HDF5 file)
	 * @returns A Map of Fragment::type_t and pointers to Fragments, suitable for ArtdaqInput
	 *
	 * This function is pure virtual.
	 */
	virtual std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() = 0;
	/**
	 * @brief Read a RawEventHeader from the Dataset (HDF5 file)
	 * @param seqID Sequence ID of the RawEventHeader (should be equivalent to event number)
	 * @return Pointer to a RawEventHeader if a match was found in the Dataset, nullptr otherwise
	 *
	 * This function is pure virtual.
	 */
	virtual std::unique_ptr<artdaq::detail::RawEventHeader> getEventHeader(artdaq::Fragment::sequence_id_t const& seqID) = 0;

protected:
	FragmentDatasetMode mode_;  ///< Mode of this FragmentDataset, either FragmentDatasetMode::Write or FragmentDatasetMode::Read
	std::shared_ptr<artdaq::FragmentNameHelper> nameHelper_; ///< FragmentNameHelper used to translate between Fragment Type and string instance names

private:
	FragmentDataset(FragmentDataset const&) = delete;
	FragmentDataset(FragmentDataset&&) = delete;
	FragmentDataset& operator=(FragmentDataset const&) = delete;
	FragmentDataset& operator=(FragmentDataset&&) = delete;
};

}  // namespace hdf5
}  // namespace artdaq

/** \cond */

#ifndef EXTERN_C_FUNC_DECLARE_START
#define EXTERN_C_FUNC_DECLARE_START extern "C" {
#endif

#define DEFINE_ARTDAQ_DATASET_PLUGIN(klass)                                            \
	EXTERN_C_FUNC_DECLARE_START                                                        \
	std::unique_ptr<artdaq::hdf5::FragmentDataset> make(fhicl::ParameterSet const& ps) \
	{                                                                                  \
		return std::unique_ptr<artdaq::hdf5::FragmentDataset>(new klass(ps));          \
	}                                                                                  \
	}

/** \endcond */

#endif  // artdaq_demo_hdf5_hdf5_FragmentDataset_hh
