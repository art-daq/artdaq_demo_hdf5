
if( NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/HighFive )
    message("Cloning HighFive into ${CMAKE_CURRENT_BINARY_DIR}/HighFive")
    execute_process(COMMAND git clone --branch v2.2.1 https://github.com/BlueBrain/HighFive.git WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    include_directories(${CMAKE_CURRENT_BINARY_DIR}/HighFive)
endif()

cet_build_plugin(highFiveNtupleDataset dataset )

cet_build_plugin(highFiveGroupedDataset dataset )

cet_build_plugin(highFiveGeoCmpltPDSPSample dataset )

cet_build_plugin(highFiveGeoSplitPDSPSample dataset )

cet_build_plugin(highFiveTimeBasedPDSPSample dataset)

install_headers()
install_source()