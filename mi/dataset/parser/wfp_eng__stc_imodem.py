#!/usr/bin/env python

"""
@package mi.dataset.parser.wfp_eng__stc_imodem
@file marine-integrations/mi/dataset/parser/wfp_eng__stc_imodem.py
@author Emily Hahn
@brief Parser for the WFP_ENG__STC_IMODEM dataset driver
Release notes:

initial release
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import copy
import re
import ntplib
import struct

from mi.core.log import get_logger ; log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle, DataParticleKey
from mi.core.exceptions import SampleException, DatasetParserException
from mi.dataset.parser.WFP_E_file_common import WfpEFileParser, StateKey
from mi.dataset.parser.WFP_E_file_common import HEADER_BYTES, SAMPLE_BYTES, STATUS_BYTES, PROFILE_MATCHER
from mi.dataset.dataset_parser import Parser

# This regex will be used to match the flags for the coastal wfp e engineering record:
# 0001 0000 0000 0000 0001 0001 0000 0000  (regex: \x00\x01\x00{7}\x01\x00\x01\x00{4})
# followed by 8 bytes of variable timestamp data (regex: [\x00-\xff]{8})
WFP_E_COASTAL_FLAGS_HEADER_REGEX = b'(\x00\x01\x00{7}\x01\x00\x01\x00{4})([\x00-\xff]{8})'
WFP_E_COASTAL_FLAGS_HEADER_MATCHER = re.compile(WFP_E_COASTAL_FLAGS_HEADER_REGEX)

class DataParticleType(BaseEnum):
    START_TIME = 'wfp_eng__stc_imodem_start_time'
    STATUS = 'wfp_eng__stc_imodem_status'
    ENGINEERING = 'wfp_eng__stc_imodem_engineering'

class Wfp_eng__stc_imodem_statusParserDataParticleKey(BaseEnum):
    INDICATOR = 'wfp_indicator'
    RAMP_STATUS = 'wfp_ramp_status'
    PROFILE_STATUS = 'wfp_profile_status'
    SENSOR_STOP = 'wfp_sensor_stop'
    PROFILE_STOP = 'wfp_profile_stop'


class Wfp_eng__stc_imodem_statusParserDataParticle(DataParticle):
    """
    Class for parsing data from the WFP_ENG__STC_IMODEM data set
    """

    _data_particle_type = DataParticleType.STATUS

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        match_prof = PROFILE_MATCHER.match(self.raw_data)

        if not match_prof:
            raise SampleException("Wfp_eng__stc_imodem_statusParserDataParticle: No regex match of parsed sample data: [%s]",
                                  self.raw_data)

        try:
            fields_prof = struct.unpack('>ihhII', match_prof.group(0))
            indicator = int(fields_prof[0])
            ramp_status = int(fields_prof[1])
            profile_status = int(fields_prof[2])
            profile_stop = int(fields_prof[3])
            sensor_stop = int(fields_prof[4])
        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, match_prof.group(0)))

        result = [self._encode_value(Wfp_eng__stc_imodem_statusParserDataParticleKey.INDICATOR, indicator, int),
                  self._encode_value(Wfp_eng__stc_imodem_statusParserDataParticleKey.RAMP_STATUS, ramp_status, int),
                  self._encode_value(Wfp_eng__stc_imodem_statusParserDataParticleKey.PROFILE_STATUS, profile_status, int),
                  self._encode_value(Wfp_eng__stc_imodem_statusParserDataParticleKey.SENSOR_STOP, sensor_stop, int),
                  self._encode_value(Wfp_eng__stc_imodem_statusParserDataParticleKey.PROFILE_STOP, profile_stop, int)]
        log.debug('Wfp_eng__stc_imodem_statusParserDataParticle: particle=%s', result)
        return result


class Wfp_eng__stc_imodem_startParserDataParticleKey(BaseEnum):
    SENSOR_START = 'wfp_sensor_start'
    PROFILE_START = 'wfp_profile_start'

class Wfp_eng__stc_imodem_startParserDataParticle(DataParticle):
    """
    Class for parsing data from the WFP_ENG__STC_IMODEM data set
    """

    _data_particle_type = DataParticleType.START_TIME

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        match = WFP_E_COASTAL_FLAGS_HEADER_MATCHER.match(self.raw_data)

        if not match:
            raise SampleException("Wfp_eng__stc_imodem_startParserDataParticle: No regex match of parsed sample data: [%s]",
                                  self.raw_data)

        try:
            fields = struct.unpack('>II', match.group(2))
            sensor_start = int(fields[0])
            profile_start = int(fields[1])
            log.debug('Unpacked sensor start %d, profile start %d', sensor_start, profile_start)
        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, match.group(0)))

        result = [self._encode_value(Wfp_eng__stc_imodem_startParserDataParticleKey.SENSOR_START, sensor_start, int),
                  self._encode_value(Wfp_eng__stc_imodem_startParserDataParticleKey.PROFILE_START, profile_start, int)]
        log.debug('Wfp_eng__stc_imodem_startParserDataParticle: particle=%s', result)
        return result


class Wfp_eng__stc_imodem_engineeringParserDataParticleKey(BaseEnum):
    TIMESTAMP = 'wfp_timestamp'
    PROF_CURRENT = 'wfp_prof_current'
    PROF_VOLTAGE = 'wfp_prof_voltage'
    PROF_PRESSURE = 'wfp_prof_pressure'

class Wfp_eng__stc_imodem_engineeringParserDataParticle(DataParticle):
    """
    Class for parsing data from the WFP_ENG__STC_IMODEM data set
    """

    _data_particle_type = DataParticleType.ENGINEERING

    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        # data sample can be any bytes, no need to check regex
        if len(self.raw_data) < SAMPLE_BYTES:
            raise SampleException("Wfp_eng__stc_imodem_engineeringParserDataParticle: Not enough bytes of sample data: [%s]",
                                  self.raw_data)
        try:
            fields = struct.unpack('>Ifff', self.raw_data[:16])
            timestamp = int(fields[0])
            profile_current = float(fields[1])
            profile_voltage = float(fields[2])
            profile_pressure = float(fields[3])
        except(ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, self.raw_data[:16]))

        result = [self._encode_value(Wfp_eng__stc_imodem_engineeringParserDataParticleKey.TIMESTAMP, timestamp, int),
                  self._encode_value(Wfp_eng__stc_imodem_engineeringParserDataParticleKey.PROF_CURRENT, profile_current, float),
                  self._encode_value(Wfp_eng__stc_imodem_engineeringParserDataParticleKey.PROF_VOLTAGE, profile_voltage, float),
                  self._encode_value(Wfp_eng__stc_imodem_engineeringParserDataParticleKey.PROF_PRESSURE, profile_pressure, float)]
        log.debug('Wfp_eng__stc_imodem_engineeringParserDataParticle: particle=%s', result)
        return result


class Wfp_eng__stc_imodemParser(WfpEFileParser):

    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 *args, **kwargs):
        self._saved_header = None
        super(Wfp_eng__stc_imodemParser, self).__init__(config,
                                            state,
                                            stream_handle,
                                            state_callback,
                                            publish_callback,
                                            *args, **kwargs)

    def set_state(self, state_obj):
        """
        initialize the state
        """
        log.trace("Attempting to set state to: %s", state_obj)
        if not isinstance(state_obj, dict):
            raise DatasetParserException("Invalid state structure")
        if not (StateKey.POSITION in state_obj):
            raise DatasetParserException("Invalid state keys")
        self._chunker.clean_all_chunks()
        self._record_buffer = []
        self._saved_header = None
        self._state = state_obj
        self._read_state = state_obj
        self._stream_handle.seek(state_obj[StateKey.POSITION])

    def _parse_header(self):
        """
        Parse the start time of the profile and the sensor
        """
        # read the first bytes from the file
        header = self._stream_handle.read(HEADER_BYTES)
        # parse the header
        if WFP_E_COASTAL_FLAGS_HEADER_MATCHER.match(header):
            match = WFP_E_COASTAL_FLAGS_HEADER_MATCHER.match(header)
            # use the profile start time as the timestamp
            fields = struct.unpack('>II', match.group(2))
            timestamp = int(fields[1])
            self._timestamp = float(ntplib.system_to_ntp_time(timestamp))
            sample = self._extract_sample(Wfp_eng__stc_imodem_startParserDataParticle,
                                          None,
                                          header, self._timestamp)
            # store this in case we need the data to calculate other timestamps
            self._profile_start_stop_data = fields
            if sample:
                # create particle
                self._increment_state(HEADER_BYTES)
                log.debug("Extracting header %s with read_state: %s", sample, self._read_state)
                self._saved_header = (sample, copy.copy(self._read_state))
        else:
            raise SampleException("File header does not match header regex")

    def parse_record(self, record):
        """
        determine if this is a engineering or data record and parse
        """
        sample = None
        result_particle = []
        if PROFILE_MATCHER.match(record):
            # send to WFP_eng_profiler if WFP
            match = PROFILE_MATCHER.match(record)
            fields = struct.unpack('>ihhII', match.group(0))
            # use the profile stop time
            timestamp = int(fields[3])
            self._timestamp = float(ntplib.system_to_ntp_time(timestamp))
            sample = self._extract_sample(Wfp_eng__stc_imodem_statusParserDataParticle, None,
                                          record, self._timestamp)
            self._increment_state(STATUS_BYTES)
        else:
            # pull out the timestamp for this record
            fields = struct.unpack('>I', record[:4])
            timestamp = int(fields[0])
            self._timestamp = float(ntplib.system_to_ntp_time(timestamp))
            log.trace("Converting record timestamp %f to ntp timestamp %f", timestamp, self._timestamp)
            sample = self._extract_sample(Wfp_eng__stc_imodem_engineeringParserDataParticle, None,
                                          record, self._timestamp)
            self._increment_state(SAMPLE_BYTES)
        if sample:
            # create particle
            log.trace("Extracting sample %s with read_state: %s", sample, self._read_state)
            result_particle = (sample, copy.copy(self._read_state))

        return result_particle

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """
        result_particles = []

	# header gets read in initialization, but need to send it back from parse_chunks
	if self._saved_header:
	    result_particles.append(self._saved_header)
	    self._saved_header = None

        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        while (chunk != None):
            result_particle = self.parse_record(chunk)
            if result_particle:
                result_particles.append(result_particle)

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles

    def handle_non_data(self, non_data, non_end, start):
        """
        This method handles any non-data that is found in the file
        """
        # if non-data is expected, handle it here, otherwise it is an error
        if non_data is not None and non_end <= start:
            # if this non-data is an error, send an UnexpectedDataException and increment the state
            self._increment_state(len(non_data))
            # if non-data is a fatal error, directly call the exception, if it is not use the _exception_callback
            self._exception_callback(UnexpectedDataException("Found %d bytes of un-expected non-data %s" %
                                                             (len(non_data), non_data)))


