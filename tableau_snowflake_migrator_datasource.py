'''
Tableau Workbook Redshift->Snowflake Migrator
=============================================
This is heavily based on previous work here https://github.com/calogica/tableau-redshift-snowflake-converter/blob/master/Tableau%20Redshift%20to%20Snowflake%20Migration%20Script.ipynb
This script converts Tableau packaged workbooks from Redshift to Snowflake.  It works by
parsing the workbook XML.  It will replace datasource connections so they point to your
Snowflake instance instead of Redshift.  Additionally, the script will UPCASE schemas,
tables, and any columns that don't have characters that need to be quoted.
Known limitations:
  - This will not work on converting published data sources.
  - Reports that use custom SQL may require additional manual configuration.
Usage:
  Ensure the following environment variables are set:
    SNOWFLAKE_ACCOUNT_NAME - Name of the snowflake account (not including the snowflakecomputing.com bit)
    SNOWFLAKE_USER_NAME - Snowflake user account name
    SNOWLFAKE_ROLE_NAME = Role to use for user (uppercase)
    SNOWFLAKE_DB_NAME - Name of Snowflake database (uppercase)
    SNOWFLAKE_WAREHOUSE - Snowflake warehouse (uppercase)
    SNOWFLAKE_SCHEMA - Snowflake schema (uppercase)
  Run the script and specify the path to the Tableau workbook, relative to this script:
    >>> python tableau_snowflake_migrator_datasource.py Geomarketing_HERE_GR.tdsx
'''

import sys
import io
import os
import zipfile
import shutil
import re
import logging
import xml.etree.ElementTree

ACCOUNT_NAME = 'goodyear-emea_analytics'
USER_NAME = 'LD60504'
DB_NAME = 'TST'
WAREHOUSE = ''
SCHEMA = 'EU'
ROLE_NAME = ''
AUTHENTICATION ='EQvJZoBP8tu8c$2Y@sjS'

logging.basicConfig()
LOG = logging.getLogger('migrator')
LOG.setLevel(logging.INFO)


def migrate_to_snowflake(packaged_datasource_path):
    unpacked_workbook = _unpack(packaged_datasource_path)
    workbook_xml = _migrate_xml(unpacked_workbook['datasource_path'])
    _save_migrated_workbook(workbook_xml, unpacked_workbook['datasource_path'])
    _repack(unpacked_workbook['extract_dir'], file_ext=unpacked_workbook['file_ext'])


def _unpack(packaged_datasource_path):
    file_ext = f'.{packaged_datasource_path.split(".")[-1]}'
    packaged_datasource_path = os.path.abspath(packaged_datasource_path)
    extract_dir = packaged_datasource_path.replace(file_ext, '')

    if not os.path.exists(packaged_datasource_path):
        raise Exception(f'File {packaged_datasource_path} does not exist')

    with zipfile.ZipFile(packaged_datasource_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    LOG.info('Unzipped %s to %s', packaged_datasource_path, extract_dir)

    file_ext_wb = file_ext.strip('x')

    file_path_wb = packaged_datasource_path.replace(file_ext, file_ext_wb)
    file_path_wb = os.path.join(extract_dir, os.path.basename(file_path_wb))

    return {'extract_dir': extract_dir, 'datasource_path': file_path_wb, 'file_ext': file_ext}


def _repack(extract_dir, file_ext='.tdsx'):
    zip_filename = f'{extract_dir}_Snowflake{file_ext}'
    LOG.info('Repackaging as %s', zip_filename)

    zipf = zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, extract_dir)
            zipf.write(full_path, arcname=rel_path)


def _migrate_xml(datasource_path):
    tree, replace_vars = _xml_replacements(datasource_path)
    doc = _global_replacements(tree, replace_vars)
    return doc


def _xml_replacements(datasource_path):
    tree = xml.etree.ElementTree.parse(datasource_path)
    root = tree.getroot()
    replace_vars = {}

    print('what')
    print(root.findall('.//connection'))
    for named_connection in root.findall('.//named-connection'):
        LOG.debug(
            'Replacing %s %s %s',
            named_connection.tag,
            named_connection.get('name'),
            named_connection.get('caption'),
        )

        named_connection.set('caption', f'{ACCOUNT_NAME}.snowflakecomputing.com')
        named_connection.set(
            'name', named_connection.get('name').replace('redshift', 'snowflake')
        )

        LOG.debug(
            '>> with %s %s %s',
            named_connection.tag,
            named_connection.get('name'),
            named_connection.get('caption'),
        )

        for connection in named_connection.iter('connection'):
            connection.set('authentication', f'{AUTHENTICATION}')
            connection.set('class', 'snowflake')
            connection.set('schema', f'{SCHEMA}')
            connection.set('dbname', f'{DB_NAME}')
            connection.set('server', f'{ACCOUNT_NAME}.snowflakecomputing.com')
            connection.set('service', f'{ROLE_NAME}')
            connection.set('username', f'{USER_NAME}')
            connection.set('warehouse', f'{WAREHOUSE}')
            connection.set('port', '')

    for relation in root.iter('relation'):

        if relation.get('connection') is None:
            continue
        relation.set('connection', relation.get('connection').replace('redshift', 'snowflake'))
        redshift_name = relation.get('name')
        snowflake_name = redshift_name.upper()
        replace_vars[redshift_name] = snowflake_name
        relation.set('name', snowflake_name)
        if relation.get('table') is None:
            continue
        relation.set('table', relation.get('table').upper())

    for relation_aux in root.iter('_.fcp.ObjectModelEncapsulateLegacy.true...relation'):
        if relation_aux.get('connection') is None:
            continue
        relation_aux.set('connection', relation_aux.get('connection').replace('redshift', 'snowflake'))
        if relation_aux.get('table') is None:
            continue
        relation_aux.set('table', relation_aux.get('table').upper())

    for relation_aux2 in root.iter('_.fcp.ObjectModelEncapsulateLegacy.false...relation'):
        # To be in line with top logic
        if relation_aux2.get('connection') is None:
            continue
        relation_aux2.set('connection', relation_aux2.get('connection').replace('redshift', 'snowflake'))
        if relation_aux2.get('table') is None:
            continue
        relation_aux2.set('table', relation_aux2.get('table').upper())

    for metadata_record in root.findall('.//metadata-record'):
        if metadata_record.get('class') == 'column':
            for metadata in metadata_record:  # revisar!
                if metadata.tag == 'remote-name':
                    snowflake_value = metadata.text
                    has_quotables = re.search(r'[^a-z0-9_]', metadata.text) is not None
                    starts_with_num = re.search(r'^[0-9]', metadata.text) is not None
                    #if not (has_quotables or starts_with_num):
                    snowflake_value = metadata.text.upper()
                    replace_vars[metadata.text] = snowflake_value

                    LOG.debug(
                        'Replacing %s %s with %s', metadata.tag, metadata.text, snowflake_value
                    )
                    metadata.text = snowflake_value

                if metadata.tag == 'parent-name':
                    metadata.text = metadata.text.upper()

    for column_rename in root.findall('.//column'):
        if column_rename.get('caption') is not None:
            if column_rename.get('caption') == 'Country Code':
                continue
            if column_rename.get('caption') == 'countryCode':
                continue
            print(column_rename.get('caption'))
            snowflake_value = column_rename.get('caption').upper()
            column_rename.set('caption', snowflake_value)


    return (tree, replace_vars)




def _global_replacements(tree, replace_vars):
    with io.BytesIO() as bs:
        tree.write(bs)
        doc = bs.getvalue().decode()

    for redshift_value, snowflake_value in replace_vars.items():
        doc = doc.replace(f'[{redshift_value}]', f'[{snowflake_value}]')
    return doc


def _save_migrated_workbook(doc, file_path_wb):
    with open(file_path_wb, 'w') as xmlfile:
        xmlfile.write(doc)


if __name__ == '__main__':
    migrate_to_snowflake(sys.argv[1])
