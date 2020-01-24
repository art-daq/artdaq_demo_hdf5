#ifndef artdaq_demo_hdf5_hdf5_FragmentDataset_hh
#define artdaq_demo_hdf5_hdf5_FragmentDataset_hh 1

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "fhiclcpp/ParameterSet.h"

namespace artdaq {
namespace hdf5 {

class FragmentDataset
{
public:
	FragmentDataset(fhicl::ParameterSet const&) {}
	void insert(artdaq::Fragment const& f) { T.insert(f); }
	void insert(artdaq::detail::RawEventHeader const&) = 0;
};

}  // namespace hdf5
}  // namespace artdaq

/** \cond */

#ifndef EXTERN_C_FUNC_DECLARE_START
#define EXTERN_C_FUNC_DECLARE_START extern "C" {
#endif

#define DEFINE_ARTDAQ_DATASET_PLUGIN(klass)                                         \
	EXTERN_C_FUNC_DECLARE_START                                                     \
	std::unique_ptr<artdaq::TransferInterface> make(fhicl::ParameterSet const& ps)  \
	{                                                                               \
		return std::unique_ptr<artdaq::hdf5::FragmentDataset>(new klass(ps, role)); \
	}                                                                               \
	}

/** \endcond */

#endif  // artdaq_demo_hdf5_hdf5_FragmentDataset_hh