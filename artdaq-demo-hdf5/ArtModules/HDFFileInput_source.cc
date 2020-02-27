#include "art/Framework/Core/InputSourceMacros.h"
#include "art/Framework/IO/Sources/Source.h"
#include "art/Framework/IO/Sources/SourceTraits.h"
#include "artdaq-demo-hdf5/ArtModules/detail/HDFFileReader.hh"

#include <string>
using std::string;

namespace art {
/**
	* \brief  Specialize an art source trait to tell art that we don't care about
	* source.fileNames and don't want the files services to be used.
	*/
template<>
struct Source_generator<artdaq::detail::HDFFileReader>
{
	static constexpr bool value = true;  ///< Used to suppress use of file services on art Source
};
}  // namespace art
namespace artdaq {
/**
	 * \brief HDFFileInput is a typedef of art::Source<detail::HDFFileReader>
	 */
typedef art::Source<detail::HDFFileReader> HDFFileInput;
}  // namespace artdaq

DEFINE_ART_INPUT_SOURCE(artdaq::HDFFileInput)
