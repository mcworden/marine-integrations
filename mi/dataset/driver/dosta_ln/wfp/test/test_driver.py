"""
@package mi.dataset.driver.dosta_ln.wfp.test.test_driver
@file marine-integrations/mi/dataset/driver/dosta_ln/wfp/driver.py
@author Mark Worden
@brief Test cases for dosta_ln_wfp driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/dsa/test_driver
       $ bin/dsa/test_driver -i [-t testname]
       $ bin/dsa/test_driver -q [-t testname]
"""

__author__ = 'Mark Worden'
__license__ = 'Apache 2.0'

import unittest

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.log import get_logger ; log = get_logger()
from mi.idk.exceptions import SampleTimeout

from mi.idk.dataset.unit_test import DataSetTestCase
from mi.idk.dataset.unit_test import DataSetIntegrationTestCase
from mi.idk.dataset.unit_test import DataSetQualificationTestCase
from mi.dataset.dataset_driver import DriverParameter

from mi.dataset.dataset_driver import DataSourceConfigKey, DataSetDriverConfigKeys
from mi.dataset.driver.dosta_ln.wfp.driver import DostaLnWfpDataSetDriver
from mi.dataset.parser.dosta_ln_wfp import HEADER_BYTES, DataParticleType, WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_BYTES
from pyon.agent.agent import ResourceAgentState
from interface.objects import ResourceAgentErrorEvent

# Fill in driver details
DataSetTestCase.initialize(
    driver_module='mi.dataset.driver.dosta_ln.wfp.driver',
    driver_class='DostaLnWfpDataSetDriver',
    agent_resource_id = '123xyz',
    agent_name = 'Agent007',
    agent_packet_config = DostaLnWfpDataSetDriver.stream_config(),
    startup_config = {
        DataSourceConfigKey.RESOURCE_ID: 'dosta_ln_wfp',
        DataSourceConfigKey.HARVESTER:
        {
            DataSetDriverConfigKeys.DIRECTORY: '/tmp/dsatest',
            DataSetDriverConfigKeys.PATTERN: 'E*.DAT',
            DataSetDriverConfigKeys.FREQUENCY: 1,
        },
        DataSourceConfigKey.PARSER: {}
    }
)

SAMPLE_STREAM = 'dosta_ln_wfp_parsed'

# The integration and qualification tests generated here are suggested tests,
# but may not be enough to fully test your driver. Additional tests should be
# written as needed.

###############################################################################
#                            INTEGRATION TESTS                                #
# Device specific integration tests are for                                   #
# testing device specific capabilities                                        #
###############################################################################
@attr('INT', group='mi')
class IntegrationTest(DataSetIntegrationTestCase):
 
    def test_get(self):
        """
        Test that we can get data from files.  Verify that the driver
        sampling can be started and stopped
        """

        self.clear_sample_data()

        # Start sampling and watch for an exception
        self.driver.start_sampling()

        self.clear_async_data()
        self.create_sample_data('E0000001.DAT', "E0000001.DAT")
        self.assert_data(None, 'first.yml', count=1, timeout=10)

        # Read the remaining values in first.DAT
        self.assert_data(None, None, count=682, timeout=100)

        self.clear_async_data()
        self.create_sample_data('E0000002.DAT', "E0000002.DAT")
        self.assert_data(None, 'six_samples.yml', count=6, timeout=10)

        # Read the remaining values in second.DAT
        self.assert_data(None, None, count=677, timeout=100)

        self.clear_async_data()
        self.create_sample_data('E0000001.DAT', "E0000003.DAT")
        # start is the same particle here, just use the same results
        self.assert_data(None, count=30, timeout=10)

    def test_stop_resume(self):
        """
        Test the ability to stop and restart the process
        """
        path_1 = self.create_sample_data('E0000001.DAT', "E0000001.DAT")
        path_2 = self.create_sample_data('E0000002.DAT', "E0000002.DAT")

        # Create and store the new driver state
        # The first file will be fully ingested
        # The second file will have had 4 samples read from it already
        state = {
            'E0000001.DAT': self.get_file_state(path_1, True, None),
            'E0000002.DAT': self.get_file_state(path_2, False,
                                                HEADER_BYTES+(WFP_E_GLOBAL_RECOVERED_ENG_DATA_SAMPLE_BYTES*4))
        }
        self.driver = self._get_driver_object(memento=state)

        # create some data to parse
        self.clear_async_data()

        self.driver.start_sampling()

        # verify data is produced
        self.assert_data(None, 'two_samples.yml', count=2, timeout=10)


    def test_stop_start_resume(self):
        """
        Test the ability to stop and restart sampling, ingesting files in the
        correct order
        """
        self.clear_async_data()

        self.driver.start_sampling()

        self.create_sample_data('E0000001.DAT', "E0000001.DAT")
        self.create_sample_data('E0000002.DAT', "E0000002.DAT")

        # Read the first record and compare it
        self.assert_data(None, 'first.yml', count=1, timeout=10)

        # Read the the remaining 682 records from E0000001.DAT, plus 4 additional from E0000002.DAT
        self.assert_data(None, None, count=686, timeout=100)

        # Ensure it is true that E0000001.DAT was fully ingested
        self.assert_file_ingested("E0000001.DAT")

        # Ensure it is true that E0000002.DAT was NOT fully ingested
        self.assert_file_not_ingested("E0000002.DAT")

        self.driver.stop_sampling()
        self.driver.start_sampling()

        # Read 2 records and compare them
        self.assert_data(None, 'two_samples.yml', count=2, timeout=10)

        # Read the remaining 677 records from E0000002.DAT
        self.assert_data(None, None, count=677, timeout=100)

        # Ensure it is true that E0000002.DAT was fully ingested
        self.assert_file_ingested("E0000002.DAT")

    def test_sample_exception(self):
        """
        Test a case that should produce a sample exception and confirm the
        sample exception occurs
        """
        self.clear_async_data()

        config = self._driver_config()['startup_config']['harvester']['pattern']
        filename = config.replace("*", "foo")
        self.create_sample_data(filename)

        # Start sampling and watch for an exception
        self.driver.start_sampling()
        # an event catches the sample exception
        self.assert_event('ResourceAgentErrorEvent')
        self.assert_file_ingested(filename)


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################
@attr('QUAL', group='mi')
class QualificationTest(DataSetQualificationTestCase):

    def test_publish_path(self):
        """
        Setup an agent/driver/harvester/parser and verify that data is
        published out the agent
        """
        self.create_sample_data('small.DAT', 'E0000001.DAT')
        # self.assert_initialize(final_state=ResourceAgentState.COMMAND)
        #
        # # NOTE: If the processing is not slowed down here, the engineering samples are
        # # returned in the wrong order
        # self.dataset_agent_client.set_resource({DriverParameter.RECORDS_PER_SECOND: 1})
        # self.assert_start_sampling()

        # Puts into SAMPLING mode
        self.assert_initialize()

        # Verify we get one sample
        try:
            # Get fours records
            result = self.data_subscribers.get_samples(DataParticleType.INSTRUMENT, 6)
            log.debug("First RESULT: %s", result)

            # Verify values
            self.assert_data_values(result, 'six_samples.yml')
        except Exception as e:
            log.error("Exception trapped: %s", e)
            self.fail("Sample timeout.")


    def test_large_import(self):
        """
        Test importing a large number of samples from the file at once
        """
        self.create_sample_data('E0000002.DAT', 'E0000002.DAT')
        self.assert_initialize()

        try:
            # Attempt to retrieve 400 samples and use a 500 second timeout
            self.data_subscribers.get_samples(DataParticleType.INSTRUMENT, 400, 500)

            # There should be 283 elements remaining in the queue
            self.assert_sample_queue_size(DataParticleType.INSTRUMENT, 283)

            # Attempt to retrieve the remaining 283 samples using a 500 second timeout
            self.data_subscribers.get_samples(DataParticleType.INSTRUMENT, 283, 500)

        except Exception as e:
            log.error("Exception trapped: %s", e)
            self.fail("Sample timeout.")


    def test_stop_start(self):
        """
        Test the agents ability to start data flowing, stop, then restart
        at the correct spot.
        """
        log.info("CONFIG: %s", self._agent_config())
        self.create_sample_data('one.DAT', "E0000001.DAT")

        self.assert_initialize()

        # Verify we get one sample
        try:
            # Read the first file containing one record and verify the data
            result = self.data_subscribers.get_samples(DataParticleType.INSTRUMENT, 1)
            log.debug("RESULT 1: %s", result)

            # Verify values
            self.assert_data_values(result, 'first.yml')

            # Make sure the queue is now emtpy
            self.assert_sample_queue_size(DataParticleType.INSTRUMENT, 0)

            self.create_sample_data('small.DAT', "E0000002.DAT")
            # Now read the first four records of the second file then stop
            result2 = self.data_subscribers.get_samples(DataParticleType.INSTRUMENT, 4)
            log.debug("RESULT 2: %s", result2)
            self.assert_stop_sampling()

            # There should be 2 items remaining on the queue
            self.assert_sample_queue_size(DataParticleType.INSTRUMENT, 2)

            # Restart sampling and ensure we get the last 2 records of the file
            self.assert_start_sampling()
            result3 = self.data_subscribers.get_samples(DataParticleType.INSTRUMENT, 2)
            log.debug("RESULT 3: %s", result3)
            result.extend(result3)
            self.assert_data_values(result3, 'two_samples.yml')

            # There should be 0 elements in the queue
            self.assert_sample_queue_size(DataParticleType.INSTRUMENT, 0)

        except SampleTimeout as e:
            log.error("Exception trapped: %s", e, exc_info=True)
            self.fail("Sample timeout.")


    def test_shutdown_restart(self):
        """
        Test a full stop of the dataset agent, then restart the agent
        and confirm it restarts at the correct spot.
        """
        log.info("CONFIG: %s", self._agent_config())
        self.create_sample_data('one.DAT', "E0000001.DAT")

        self.assert_initialize()

        # Verify we get one sample
        try:
            # Read the first file containing one record and verify the data
            result = self.data_subscribers.get_samples(DataParticleType.INSTRUMENT, 1)
            log.debug("RESULT 1: %s", result)

            # Verify values
            self.assert_data_values(result, 'first.yml')

            # Make sure the queue is now emtpy
            self.assert_sample_queue_size(DataParticleType.INSTRUMENT, 0)

            self.create_sample_data('small.DAT', "E0000002.DAT")
            # Now read the first four records of the second file then stop
            result2 = self.data_subscribers.get_samples(DataParticleType.INSTRUMENT, 4)
            log.debug("RESULT 2: %s", result2)
            self.assert_stop_sampling()

            # There should be 2 items remaining on the queue
            self.assert_sample_queue_size(DataParticleType.INSTRUMENT, 2)

            # stop the agent
            self.stop_dataset_agent_client()
            # re-start the agent
            self.init_dataset_agent_client()
            #re-initialize
            self.assert_initialize(final_state=ResourceAgentState.COMMAND)
            # Restart sampling and ensure we get the last 2 records of the file
            self.assert_start_sampling()

            result3 = self.data_subscribers.get_samples(DataParticleType.INSTRUMENT, 2)
            log.debug("RESULT 3: %s", result3)
            result.extend(result3)
            self.assert_data_values(result3, 'two_samples.yml')

            # There should be 0 elements in the queue
            self.assert_sample_queue_size(DataParticleType.INSTRUMENT, 0)

        except SampleTimeout as e:
            log.error("Exception trapped: %s", e, exc_info=True)
            self.fail("Sample timeout.")

    def test_parser_exception(self):
        """
        Test an exception is raised after the driver is started during
        record parsing.
        """
        self.clear_sample_data()
        self.create_sample_data('E0000001-BAD-DATA.DAT', 'E0000001.DAT')
        self.create_sample_data('one.DAT', 'E0000002.DAT')

        self.assert_initialize()

        self.event_subscribers.clear_events()
        result = self.get_samples(DataParticleType.INSTRUMENT, 1)

        self.assert_data_values(result, 'first.yml')

        # There should be 0 elements in the queue
        self.assert_sample_queue_size(DataParticleType.INSTRUMENT, 0)

        # Verify an event was raised and we are in our retry state
        self.assert_event_received(ResourceAgentErrorEvent, 10)
        self.assert_state_change(ResourceAgentState.STREAMING, 10)


