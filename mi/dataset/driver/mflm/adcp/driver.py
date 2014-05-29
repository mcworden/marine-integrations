"""
@package mi.dataset.driver.mflm.adcp.driver
@file marine-integrations/mi/dataset/driver/mflm/adcp/driver.py
@author Emily Hahn
@brief Driver for the mflm_adcp
Release notes:

Initial version.
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'


from mi.core.log import get_logger ; log = get_logger()
from mi.dataset.driver.sio_mule.sio_mule_single_driver import SioMuleSingleDataSetDriver
from mi.dataset.parser.adcps import AdcpsParser, AdcpsParserDataParticle

class MflmADCPSDataSetDriver(SioMuleSingleDataSetDriver):
    
    @classmethod
    def stream_config(cls):
        return [AdcpsParserDataParticle.type()]

    def _build_parser(self, parser_state, infile):
        """
        Build and return the parser
        """
        config = self._parser_config
        config.update({
            'particle_module': 'mi.dataset.parser.adcps',
            'particle_class': 'AdcpsParserDataParticle'
        })
        log.debug("MYCONFIG: %s", config)
        self._parser = AdcpsParser(
            config,
            parser_state,
            infile,
            self._save_parser_state,
            self._data_callback,
            self._sample_exception_callback
        )
        return self._parser


