from configparser import ConfigParser
import os


class ConfigReader:

    def get_config(self, enterprise: bool, section: str, variable: str) -> str:
        config = ConfigParser()
        # This is to correct the path if calling from bin or mint
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        if enterprise:
            config.read(path + '\\' + 'config/config.cfg')
            result = config.get(section, variable)
            return result
        else:
            config.read(path + '\\' + 'config/config.cfg')
            project = config.get(section, variable)
            config.read(path + '\\' + f'config/{project}')
            return config

#cr = ConfigReader()
#mint_config = cr.get_config(False, "projects", "Mint")
#process_name = mint_config.get("general", "ProcessName")
#print(process_name)
