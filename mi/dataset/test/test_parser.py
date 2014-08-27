#!/usr/bin/env python

"""
@package mi.dataset.test.test_parser Base dataset parser test code
@file mi/dataset/test/test_driver.py
@author Steve Foley, Mark Worden
@brief Test code for the dataset parser base classes and common structures for
testing parsers.
"""
from mi.core.unit_test import MiUnitTestCase, MiIntTestCase
from mi.core.instrument.data_particle import DataParticleKey

import os
import numpy
import yaml

from mi.core.log import get_logger
log = get_logger()

EXPECTED_RESULTS_DATA_KEY = 'data'


# Make some stubs if we need to share among parser test suites
class ParserUnitTestCase(MiUnitTestCase):

    def assert_particle_data_against_yaml_contents(self, expected_results_yml_file_path, actual_particle_data_list,
                                                   specific_index=None, expected_results_offset=0):
        """
        This method verifies expected results contained within a YAML file against actual particle data in a list.
        This method will use unittest assert calls to verify the particle data.  The first assert failure will
        result in exit of this method.
        @param expected_results_yml_file_path A path to a YAML file containing the expected particle data
        @param actual_particle_data_list A list of DataParticle objects containing the actual particle data
        @param specific_index The index of the list item to compare against.  If the value is None, all samples
        in actual_particle_data_list the will be compared.
        @param expected_results_offset A offset into the expected results data list loaded from the YAML file.
        The value is defaulted to 0.
        """

        self.assertTrue(os.path.exists(expected_results_yml_file_path))

        expected_particle_data = self.get_dict_from_yml(expected_results_yml_file_path)

        for i in range(len(actual_particle_data_list)):
            if specific_index is None or i == specific_index:
                self.assert_result(expected_particle_data[EXPECTED_RESULTS_DATA_KEY][i+expected_results_offset],
                                   actual_particle_data_list[i])

    def assert_particle_data_list(self, expected_particle_data, actual_particle_data_list,
                                  specific_index=None, expected_results_offset=0):
        """
        This method verifies expected results contained within a YAML file against actual particle data in a list.
        This method will use unittest assert calls to verify the particle data.  The first assert failure will
        result in exit of this method.
        @param expected_particle_data A file path to a YAML file containing the expected particle data
        @param actual_particle_data_list A list of DataParticle objects containing the actual particle data
        @param specific_index The index of the list item to compare against.  If the value is None, all samples
        in actual_particle_data_list the will be compared.
        @param expected_results_offset A offset into the expected results data list loaded from the YAML file.
        The value is defaulted to 0.
        """

        for i in range(len(actual_particle_data_list)):
            if specific_index is None or i == specific_index:
                log.debug("Comparing value at index %d", i)
                self.assert_result(expected_particle_data[i+expected_results_offset],
                                   actual_particle_data_list[i])

    def assert_result(self, expected_particle_data, actual_particle_data):
        """
        This method verifies actual particle data against expected particle data.  This method will use unittest
        assert calls to verify the particle data.  The first assert failure will result in exit of this method.
        @param expected_particle_data An OrderedDict object containing expected particle data
        @param actual_particle_data A DataParticle object containing actual particle data
        """

        particle_dict = actual_particle_data.generate_dict()

        #for efficiency turn the particle values list of dictionaries into a dictionary
        particle_values = {}
        for param in particle_dict.get(DataParticleKey.VALUES):
            particle_values[param[DataParticleKey.VALUE_ID]] = param[DataParticleKey.VALUE]

        # compare each key in the test to the data in the particle
        for key in expected_particle_data:
            test_data = expected_particle_data[key]

            #get the correct data to compare to the test
            if key == DataParticleKey.INTERNAL_TIMESTAMP:
                particle_data = actual_particle_data.get_value(DataParticleKey.INTERNAL_TIMESTAMP)
                #the timestamp is in the header part of the particle

            else:
                particle_data = particle_values.get(key)
                #others are all part of the parsed values part of the particle

            if particle_data is None:
                #generally OK to ignore index keys in the test data, verify others

                log.warning("\nWarning: assert_result ignoring test key %s, does not exist in particle", key)
            else:

                if isinstance(test_data, float):
                    log.debug("Comparing parameter %s actual value %.10f to expected value %.10f",
                              key, particle_data, test_data)
                    # slightly different test for these values as they are floats.
                    compare = numpy.abs(test_data - particle_data) <= 1e-5
                    self.assertTrue(compare)
                else:
                    log.debug("Comparing parameter %s actual value %s to expected value %s",
                              key, particle_data, test_data)
                    # otherwise they are all ints and should be exactly equal
                    self.assertEqual(test_data, particle_data)

    @staticmethod
    def get_dict_from_yml(yml_file_path):
        """
        This utility routine loads the contents of a yml file
        into a dictionary
        """

        fid = open(yml_file_path, 'r')
        result = yaml.load(fid)
        fid.close()

        return result


class ParserIntTestCase(MiIntTestCase):
    pass