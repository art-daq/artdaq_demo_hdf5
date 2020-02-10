#ifndef artdaq_demo_hdf5_hdf5_FragmentDataset_hh
#define artdaq_demo_hdf5_hdf5_FragmentDataset_hh 1

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "fhiclcpp/ParameterSet.h"

#include <unordered_map>

namespace artdaq {
namespace hdf5 {

enum class FragmentDatasetMode : uint8_t
{
	Read = 0,
	Write = 1
};

class FragmentDataset
{
public:
	FragmentDataset(fhicl::ParameterSet const& ps, std::string mode);
	virtual ~FragmentDataset() noexcept = default;
	virtual void insertOne(Fragment const&) = 0;
	virtual void insertMany(Fragments const& fs)
	{
		for (auto f : fs) insertOne(f);
	}
	virtual void insertHeader(detail::RawEventHeader const&) = 0;
	virtual std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> readNextEvent() = 0;
	virtual std::unique_ptr<artdaq::detail::RawEventHeader> GetEventHeader(artdaq::Fragment::sequence_id_t const&) = 0;

protected:
	FragmentDatasetMode mode_;
};

}  // namespace hdf5
}  // namespace artdaq

/** \cond */

#define DEFINE_ARTDAQ_DATASET_PLUGIN(klass)                                            \
	extern "C" {                                                                       \
	std::unique_ptr<artdaq::hdf5::FragmentDataset> make(fhicl::ParameterSet const& ps) \
	{                                                                                  \
		return std::unique_ptr<artdaq::hdf5::FragmentDataset>(new klass(ps));          \
	}                                                                                  \
	}

/** \endcond */

#endif  // artdaq_demo_hdf5_hdf5_FragmentDataset_hh