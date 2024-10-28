from config_error import ConfigError

class Config:
    def __init__(self, data_file_path, login_name, password, report_name, months):
        self.data_file_path = data_file_path
        self.login_name = login_name
        self.password = password
        self.report_name = report_name
        self.months = months

    @ property
    def data_file_path(self):
        return self._data_file_path

    @ data_file_path.setter
    def data_file_path(self, value):
        if value is None:
            raise ConfigError("Missing data file path in config.")
        else:
            self._data_file_path = value
    
    @ property
    def login_name(self):
        return self._login_name

    @ login_name.setter
    def login_name(self, value):
        if value is None:
            raise ConfigError("Missing login name in config.")
        else:
            self._login_name = value

    @ property
    def password(self):
        return self._password

    @ password.setter
    def password(self, value):
        if value is None:
            raise ConfigError("Missing password in config.")
        else:
            self._password = value

    @property
    def report_name(self):
        return self._report_name

    @report_name.setter
    def report_name(self, value):
        allowed_values = ["Inspections", "Work Orders", "Cases", "Requests", "Case Fees", "Inspection Questions", "Case Tasks", "Case Corrections"]
        if value is None:
            raise ConfigError("Missing report name in config")
        elif value in allowed_values:
            self._report_name = value            
        else:
            raise ConfigError("Invalid report name: {}. Expecting one of {}".format(
                value, ", ".join(allowed_values)))

    @property
    def months(self):
        return self._months

    @months.setter
    def months(self, value):
        if value is None:
            raise ConfigError("Missing include number of months in config")
        else:
            self._months = value