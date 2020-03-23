#ifndef artdaq_demo_hdf5_HDF5_highFive_highFiveDatasetHelper_hh
#define artdaq_demo_hdf5_HDF5_highFive_highFiveDatasetHelper_hh 1

#include "artdaq-demo-hdf5/HDF5/highFive/HighFive/include/highfive/H5DataSet.hpp"

namespace artdaq {
namespace hdf5 {

/**
	 * @brief Helper class for HighFiveNtupleDataset
	 *
	 * This class represents a column in an Ntuple-formatted group of datasets
	 */
class HighFiveDatasetHelper
{
public:
	/**
	 * @brief HighFiveDatasetHelper Constructor
	 * @param dataset HighFive::DataSet to read/write
	 * @param chunk_size Number of rows per chunk in the dataset
	 */
	HighFiveDatasetHelper(HighFive::DataSet const& dataset, size_t chunk_size = 128)
	    : dataset_(dataset)
	    , current_row_(0)
	    , current_size_(dataset.getDimensions()[0])
	    , chunk_size_(chunk_size)
	{
		// Zero-size chunks are not allowed
		if (chunk_size_ == 0)
		{
			chunk_size_ = 10;
		}
	}

	/**
	 * @brief Write a value to the column, resizing if necessary
	 * @param data Value to write
	 */
	template<typename T>
	void write(T const& data)
	{
		if (current_row_ >= current_size_) resize();

		dataset_.select({current_row_, 0}, {1, 1}).write(data);
		current_row_++;
	}

	/**
	 * @brief Write a value to the column, resizing if necessary
	 * @param data Value to write
	 * @param width Number of dataset values represented by data
	 */
	template<typename T>
	void write(T const& data, size_t width)
	{
		if (current_row_ >= current_size_) resize();

		dataset_.select({current_row_, 0}, {1, width}).write(data);
		current_row_++;
	}

	/**
	 * @brief Read a set of value from a row of the column
	 * @param row Row to read
	 * @return Vector of values read from column
	 */
	template<typename T>
	std::vector<T> read(size_t row)
	{
		if (row >= current_size_)
		{
			TLOG_ERROR("HighFiveDatasetHelper") << "Requested row " << row << " is outside the bounds of this dataset! dataset sz=" << current_size_;
			return std::vector<T>();
		}

		std::vector<T> readBuf;
		dataset_.select({row, 0}, {1, dataset_.getDimensions()[1]}).read(readBuf);
		return readBuf;
	}

	/**
	 * @brief Read a single value from the column
	 * @param row Row to read
	 * @return Value in the first slot in the column at the desginated row
	 */
	template<typename T>
	T readOne(size_t row)
	{
		auto res = read<T>(row);
		if (res.size()) return res[0];

		return T();
	}

	/**
	 * @brief Get the number of rows in the column
	 * @return The number of rows in the column
	 */
	size_t getDatasetSize() { return current_size_; }
	/**
	 * @brief Get the number of entries in each row
	 * @return The number of entries in each row
	 */
	size_t getRowSize() { return dataset_.getDimensions()[1]; }

private:
	void resize()
	{
		TLOG(TLVL_TRACE) << "HighFiveDatasetHelper::resize: Growing dataset by one chunk";
		// Ideally, grow by one chunk at a time
		if (current_size_ % chunk_size_ == 0)
		{
			dataset_.resize({current_size_ + chunk_size_, dataset_.getDimensions()[1]});
			current_size_ += chunk_size_;
		}
		else
		{
			auto size_add = chunk_size_ - (current_size_ % chunk_size_);
			if (size_add / static_cast<double>(chunk_size_) < 0.5)
			{
				size_add += chunk_size_;
			}

			dataset_.resize({current_size_ + size_add, dataset_.getDimensions()[1]});
			current_size_ += size_add;
		}
	}

private:
	HighFive::DataSet dataset_;
	size_t current_row_;
	size_t current_size_;
	size_t chunk_size_;
};
}  // namespace hdf5
}  // namespace artdaq

#endif  //artdaq_demo_hdf5_HDF5_highFive_highFiveDatasetHelper_hh