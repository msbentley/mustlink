#!/usr/bin/python
"""
mustlink.py

Mark S. Bentley (mark@lunartech.org), 2019

A module to use the MUSTlink API to query WebMUST.

Basic authentication credentials should be stored in a simple
YAML file and pointed at by the config_file parameter when
instantiated the Must class. An example is:

user:
    login: "userone"
    password: "blah"

"""

import yaml
import requests
import urllib
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as md

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

import logging
log = logging.getLogger(__name__)

default_url = 'https://bepicolombo.esac.esa.int/webclient-must/mustlink'

default_config = os.path.join(
    os.environ.get('APPDATA') or
    os.environ.get('XDG_CONFIG_HOME') or
    os.path.join(os.environ['HOME'], '.config'),
    "mustlink.yml")

date_format = '%Y-%m-%d %H:%M:%S'
date_format_ms = '%Y-%m-%d %H:%M:%S.%f'

class Must:


    def __init__(self, url=default_url, config_file=default_config):
        """The WebMUST URL instance can be specified, along with a
        YAML config file containing a user section with login and
        password entries. If neither of these are provided, the
        default values are used"""

        self.url = url
        self.config = None
        self.auth(config_file)
        self.get_providers()
        self.default_provider = None
        self.tables = None


    def _url(self, path):
        """Helper function to append the path to the base URL"""
        
        return self.url + path


    def auth(self, config_file):
        """Try to authorise with the WebMUST instance using the user
        and password specified in the config file (and loaded into
        self.config). If successful, the returned token is stored for
        use in future calls."""

        try:
            f = open(config_file, 'r')
            self.config = yaml.load(f, Loader=yaml.BaseLoader)
            try:
                r = requests.post(self._url('/auth/login'), json={
                    'username': self.config['user']['login'],
                    'password': self.config['user']['password'],
                    'maxDuration': 'false'})
                r.raise_for_status()
                self.token = r.json()['token']
            except (requests.exceptions.RequestException, ConnectionResetError) as err:
                log.error('error with authorisation: {:s}'.format(err))
        except FileNotFoundError:
            log.error('config file {:s} not found'.format(config_file))

        self.user = self.get_user()

        return


    def get_user(self):
        """Retrieves information for the currently logged-in user"""

        r = requests.get(
                self._url('/usermanagement/userinfo'),
                headers={'Authorization': self.token})
        r.raise_for_status()
        user = r.json()
        log.info('user {:s} currently logged in'.format(user['login']))

        return user


    def get_providers(self):
        """Obtains a list of so-called providers, e.g. BEPICRUISE in
        the case of BepiColombo"""
        
        r = requests.get(
                self._url('/dataproviders'),
                headers={'Authorization': self.token})
        r.raise_for_status()
        providers = r.json()
        self.providers = [p['name'] for p in providers if p['user']=='webmust']
        log.info('{:d} providers found'.format(len(self.providers)))

        return


    def check_provider(self, provider):
        """Checks that the requested provider is in the list returned
        by get_providers"""

        if provider not in self.providers:
            log.error('provider %s is not a registered data provider' % provider)
            return None
        else:
            return provider


    def set_provider(self, provider):
        """Sets a default provider. In other API calls that need a 
        provider, if provider=None this default is used instead"""
        
        check = self.check_provider(provider)
        if check is not None:
            self.default_provider = provider
        else:
            return None


    def get_provider(self, provider):
        """Used by API calls to check if the a default provider is set,
        and if not to check the provided provider (!) exists"""

        if provider is None:
            if self.default_provider is None:
                log.error('a provider must be specified')
                return
            else:
                provider = self.default_provider
        else:
            check = self.check_provider(provider)
            if check is None:
                return None
        return provider


    def get_tables(self, provider=None):
        """Retrieves the list of tables for a given provider and stores
        this in a dictionary for later use"""

        if self.tables is None:
            self.tables = {}

        provider = self.get_provider(provider)
        if provider is None:
            return None

        r = requests.get(
            self._url('/dataproviders/{:s}/tables'.format(provider)), 
            headers={'Authorization': self.token})
        r.raise_for_status()
        self.tables[provider] = r.json()
        log.info('provider {:s} has {:d} table(s)'.format(provider, len(r.json())))

        return


    def get_table_meta(self, table, provider=None):
        """Retrieves the meta-data associated with a given table and returns as json"""

        provider = self.get_provider(provider)
        if provider is None:
            return None

        if self.tables is None:
            self.get_tables(provider)

        tables = [table['dataType'] for table in self.tables[provider]]
        if table not in tables:
            log.error('table {:s} invalid for provider {:s}'.format(table, provider))
            return None

        r = requests.get(
            self._url('/dataproviders/{:s}/table/{:s}/metadata'.format(provider, table)), 
            headers={'Authorization': self.token})
        r.raise_for_status()
        table_meta = r.json()
        
        return table_meta


    def get_table_data(self, table, start_time=None, stop_time=None, search_key='name', search_text='', 
        provider=None, max_rows=1000, mode='brief', fmt='complex', quiet=False):
        """Retrieve tabular data from a WebMUST provider and format into a pandas
        DataFrame. Columns with 'time' in the title are assumed to be times, and 
        are accordingly converted to Timestamps. Setting max_rows limits the number
        of rows returned by the API (when this is excluded the API returns a maximum
        of 5000)"""

        if mode not in ['brief', 'full']:
            log.error('mode must be one of brief, full')
            return None

        if fmt not in ['simple', 'complex']:
            log.error('table must be one of simple, complex')
            return None

        provider = self.get_provider(provider)
        if provider is None:
            return None

        if self.tables is None:
            self.get_tables(provider)

        tables = [table['dataType'] for table in self.tables[provider]]
        if table not in tables:
            log.error('table {:s} invalid for provider {:s}'.format(table, provider))
            return None

        if start_time is None:
            start_time = pd.Timestamp.now() - pd.Timedelta(days=1)
        elif type(start_time) == str:
            start_time = pd.Timestamp(start_time)

        if stop_time is None:
            stop_time = pd.Timestamp.now()
        elif type(stop_time) == str:
            stop_time = pd.Timestamp(stop_time)

        r = requests.get(
            self._url('/dataproviders/{:s}/table/{:s}/data'.format(provider, table)), 
            headers={'Authorization': self.token},
            params={
                'dateFormat': 'fromTo',
                'from': start_time.strftime(date_format),
                'to': stop_time.strftime(date_format),
                'filterKeys': search_key,
                'filterValues': search_text,
                'mode': mode.upper(),
                'representation': fmt.upper(),
                'maxRows': max_rows})
        log.debug('request URL: {:s}'.format(r.url))
        log.debug('data retrieval done')

        r.raise_for_status()
        data = r.json()
        
        if len(data['data']) == 0:
            log.warn('no table data found for those inputs')
            return None

        cols = data['headers']

        table_data = pd.DataFrame([], columns=cols)
        if fmt=='complex':
            datacells = [row['dataCells'] for row in data['data']]
            
            for idx, col in enumerate(cols):
                col_vals = pd.DataFrame(datacells)[idx].apply( lambda row: row['cellValue'] )
                table_data[col] = col_vals
        else:
            table_data = pd.DataFrame(data['data'])

        time_cols = [col for col in table_data.columns if 'time' in col.lower()]
        for col in time_cols:
            table_data[col] = pd.to_datetime(table_data[col])
        
        if not quiet:
            log.info('{:d} table entries retrieved'.format(len(table_data)))
        if len(table_data) == max_rows:
            log.warn('number of rows returned equal to maximum - increase max_rows for more data')

        return table_data


    def get_table_param(self, table, param_name, start_time, provider=None, quiet=True):
        """Requests table parameters for a given parameter and timestamp. Minimal checking is 
        currently performed on the times and return codes. Data are formatted into
        a Pandas DataFrame with time conversion to UTC performed"""

        provider = self.get_provider(provider)
        if provider is None:
            return None

        if self.tables is None:
            self.get_tables(provider)

        tables = [table['dataType'] for table in self.tables[provider]]
        if table not in tables:
            log.error('table {:s} invalid for provider {:s}'.format(table, provider))
            return None

        if type(start_time) == str:
            start_time = pd.Timestamp(start_time)


        params={
            'elementId': param_name,
            'ssc': 'null',
            'timestamp': start_time.strftime(date_format_ms)[:-3]}

        
        params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        r = requests.get(self._url('/web/tables/params/{:s}/{:s}'.format(provider, table)),
            headers={'Authorization': self.token}, params=params)

        r.raise_for_status()
        data = r.json()

        if len(data['data']) == 0:
            log.warn('no data found for the given parameter and time')
            return None

        cols = data['headers']
        datacells = [row['dataCells'] for row in data['data']][0]
        table_data = pd.DataFrame(datacells)
        drop_cols = ['cellValue', 'altText', 'bgColor', 'detail', 'webpagelink', 'rowParams']
        table_data.drop(drop_cols, inplace=True, axis=1)
        table_data.columns = cols

        if not quiet:
            log.info('{:d} table entries retrieved'.format(len(table_data)))

        return table_data



    def get_aggregations(self, provider=None, id=None):

        provider = self.get_provider(provider)
        if provider is None:
            return None

        if id is not None:
            params = {
                'key': 'id',
                'value': id }
        else:
            params = {}

        r = requests.get(self._url('/dataproviders/{:s}/aggregations'.format(provider)),
            params = params,
            headers={'Authorization': self.token})
        r.raise_for_status()

        return r.json()




    def get_data(self, param_name, start_time=None, stop_time=None, provider=None, calib=False, max_pts=None):
        """Requests data for a given parameter and time-range. Minimal checking is 
        currently performed on the times and return codes. Data are formatted into
        a Pandas DataFrame with time conversion to UTC performed"""

        provider = self.get_provider(provider)
        if provider is None:
            return None

        if start_time is None:
            start_time = pd.Timestamp.now() - pd.Timedelta(days=1)
        elif type(start_time) == str:
            start_time = pd.Timestamp(start_time)

        if stop_time is None:
            stop_time = pd.Timestamp.now()
        elif type(stop_time) == str:
            stop_time = pd.Timestamp(stop_time)

        r = requests.get(self._url('/dataproviders/{:s}/parameters/data'.format(provider)),
            headers={'Authorization': self.token},
            params={
                'key': 'name',
                'values': param_name,
                'from': start_time.strftime(date_format),
                'to': stop_time.strftime(date_format),
                'calibrate': 'true' if calib else 'false',
                'chunkCount': '' if max_pts is None else max_pts})
        r.raise_for_status()

        meta = {val['key']: val['value'] for val in r.json()[0]['metadata']}
        data = pd.DataFrame.from_dict(r.json()[0]['data'])
        if len(data) == 0:
            log.warn('no data available for parameter {:s} in this time range'.format(param_name))
            return None
        data.date = pd.to_datetime(data.date, unit='ms')
        data.set_index('date', drop=True, inplace=True)
        data.rename(columns={'value': meta['name']}, inplace=True)

        if not calib:
            data.drop('calibratedValue', axis=1, inplace=True)

        log.info('{:d} values retrieved'.format(len(data)))

        return data


    def get_latest_val(self, param_name, provider=None, calib=False):
        """Retrieves the timestamp and value of the last sample for the
        specified parameter"""

        provider = self.get_provider(provider)
        if provider is None:
            return None

        meta = self.get_param_info(param_name)
        last_t = meta['Last Sample']

        r = requests.get(self._url('/dataproviders/{:s}/parameters/data'.format(provider)),
            headers={'Authorization': self.token},
            params={
                'key': 'name',
                'values': param_name,
                'from': last_t.strftime(date_format),
                'to': (last_t+pd.Timedelta(seconds=1)).strftime(date_format),
                'calibrate': 'true' if calib else 'false'})
        r.raise_for_status()

        data = pd.DataFrame.from_dict(r.json()[0]['data'])
        if len(data) == 0:
            log.warn('no data available for parameter {:s} in this time range'.format(param_name))
            return None
        data.date = pd.to_datetime(data.date, unit='ms')
        data.set_index('date', drop=True, inplace=True)
        data.rename(columns={'value': meta['Name']}, inplace=True)

        if not calib:
            data.drop('calibratedValue', axis=1, inplace=True)

        log.info('value retrieved at time {:s}'.format(last_t.strftime(date_format)))

        return data


    def plot_data(self, param_name, start_time=None, stop_time=None, provider=None, calib=False, max_pts=None):
        """Accepts the same parameters as get_data() and retrieves and plots the data as a
        time series.  Returns a matplotlib axis object."""

        meta = self.get_param_info(param_name)
        data = self.get_data(param_name, start_time, stop_time, provider, calib, max_pts)
        if data is None:
            return None

        fig, ax = plt.subplots()
        ax.scatter(data.index, data[param_name], marker='.')
        ax.set_title(meta['Description'])
        ax.set_xlabel('Date (UTC)')
        if 'Unit' in meta.index:
            ax.set_ylabel(meta['Unit'])
        else:
            ax.set_ylabel('Raw')
        ax.grid(True)
        fig.autofmt_xdate()
        xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
        ax.xaxis.set_major_formatter(xfmt)

        plt.show()

        return ax


    def get_param_info(self, param_name, provider=None, mode='simple'):
        """Return meta-data for a single parameter of interest.
        
        Setting mode='complex' will provide additional information such 
        as monitoring checks etc. but this is currently not unpacked."""

        if mode not in ['simple', 'complex']:
            log.error('mode must be either simple or complex')
            return None

        provider = self.get_provider(provider)
        if provider is None:
            return None

        r = requests.get(self._url('/dataproviders/{:s}/parameters'.format(provider)),
        headers={'Authorization': self.token},
        params={
            'key': 'name',
            'value': param_name,
            'search': 'false',
            'mode': mode.upper(),
            'parameterType': 'TM'})
        r.raise_for_status()

        matches = r.json()
        if len(matches) == 0:
            log.warning('no matches found for parameter {:s}'.format(param_name))
            return None

        if mode=='simple':
            param = pd.Series(r.json())
            param['First Sample'] = pd.NaT if param['First Sample']=='N/A' else pd.to_datetime(param['First Sample'])
            param['Last Sample'] = pd.NaT if param['Last Sample']=='N/A' else pd.to_datetime(param['Last Sample'])
        else:
            param = r.json()

        log.info('parameter info for {:s} extracted'.format(param.Description))

        return param


    def get_param_stats(self, param_name, start_time=None, stop_time=None, provider=None):

        provider = self.get_provider(provider)
        if provider is None:
            return None

        if start_time is None:
            start_time = pd.Timestamp.now() - pd.Timedelta(days=1)
        elif type(start_time) == str:
            start_time = pd.Timestamp(start_time)

        if stop_time is None:
            stop_time = pd.Timestamp.now()
        elif type(stop_time) == str:
            stop_time = pd.Timestamp(stop_time)

        r = requests.get(self._url('/dataproviders/{:s}/parameters/statistics'.format(provider)),
            headers={'Authorization': self.token},
            params={
                'key': 'name',
                'values': param_name,
                'from': start_time.strftime(date_format),
                'to': stop_time.strftime(date_format) })
        r.raise_for_status()

        stats = pd.Series(r.json())
        stats['from'] = pd.to_datetime(stats['from'])
        stats['to'] = pd.to_datetime(stats['to'])
        
        log.info('parameter statistics for {:s} extracted'.format(stats.parameter))

        return stats


    def search_parameter(self, search_text, search_by='description', provider=None):
        """The provided searche text is used to search within the parameter
        descriptions. Matching parameters are returned in a Pandas DataFrame.
        
        Search can be by name (mnemonic) or description. 
        
        Set search_by='name' or 'description'
        """

        search_by_dict = {
            'description': 'Description',
            'name': 'Name'
        }

        if search_by not in search_by_dict.keys():
            log.error('search_by must be set to either description or name')
            return None

        provider = self.get_provider(provider)
        if provider is None:
            return None

        r = requests.get(self._url('/dataproviders/{:s}/parameters'.format(provider)),
        headers={'Authorization': self.token},
        params={
            'key': search_by_dict[search_by],
            'value': search_text,
            'search': 'true',
            'mode': 'SIMPLE',
            'parameterType': 'TM'})
        r.raise_for_status()

        matches = r.json()
        if len(matches) == 0:
            log.warning('no matches found for {:s}'.format(search_text))
            return None

        params = pd.DataFrame.from_dict(r.json())
        nans = params[params['First Sample']=='N/A'].index
        params.loc[nans, 'First Sample'] = pd.NaT
        params['First Sample'] = pd.to_datetime(params['First Sample'])
        nans = params[params['Last Sample']=='N/A'].index
        params.loc[nans, 'Last Sample'] = pd.NaT
        params['Last Sample'] = pd.to_datetime(params['Last Sample'])

        log.info('{:d} parameters match search text: {:s}'.format(len(params), search_text))

        return params


    def tree_search(self, text='', fields='Name,Description', provider=None):

        provider = self.get_provider(provider)
        if provider is None:
            return None

        r = requests.get(self._url('/metadata/treesearch'),
        headers={'Authorization': self.token},
        params={
            'field': fields,
            'text': text,
            'dataproviders': provider
        })

        r.raise_for_status()

        data = None
        for d in r.json():
            if d['type'].startswith(provider):
                data = d
                break

        return data