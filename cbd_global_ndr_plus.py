"""Tracer for NDR watershed processing."""
import collections
import glob
import logging
import multiprocessing
import os
import pathlib
import subprocess
import sqlite3
import time
import threading
import urllib
import zipfile

from inspring.ndr_plus.ndr_plus import ndr_plus
from osgeo import gdal
from osgeo import osr
import ecoshard
import pandas
import pygeoprocessing
import numpy
import retrying
import taskgraph

gdal.SetCacheMax(2**27)
logging.getLogger('taskgraph').setLevel(logging.INFO)

WORKSPACE_DIR = 'cbd_workspace'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')
SCRUB_DIR = os.path.join(ECOSHARD_DIR, 'scrubbed_ecoshards')
WORK_STATUS_DATABASE_PATH = os.path.join(WORKSPACE_DIR, 'work_status.db')
COMPUTED_STATUS = 'computed'  # use when computed but not stitched
COMPLETE_STATUS = 'complete'  # use when stitched and deleted
USE_AG_LOAD_ID = 999

# All links in this dict is an ecoshard that will be downloaded to
# ECOSHARD_DIR
ECOSHARD_PREFIX = 'https://storage.googleapis.com/'

WATERSHED_ID = 'hydrosheds_15arcseconds'

# Known properties of the DEM:
DEM_ID = 'global_dem_3s'
DEM_TILE_DIR = os.path.join(ECOSHARD_DIR, 'global_dem_3s')
DEM_VRT_PATH = os.path.join(DEM_TILE_DIR, 'global_dem_3s.vrt')

# Global properties of the simulation
RETENTION_LENGTH_M = 150
K_VAL = 1.0
TARGET_CELL_LENGTH_M = 300
FLOW_THRESHOLD = int(500**2*90 / TARGET_CELL_LENGTH_M**2)
ROUTING_ALGORITHM = 'D8'
TARGET_WGS84_LENGTH_DEG = 10/3600
AREA_DEG_THRESHOLD = 0.000016 * 10  # this is 10 times larger than hydrosheds 1 "pixel" watersheds

BIOPHYSICAL_TABLE_IDS = {
    'esa_aries_rs3': 'Value',
    'nci-ndr-biophysical_table_forestry_grazing': 'ID', }

# ADD NEW DATA HERE
ECOSHARDS = {
    DEM_ID: f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/global_dem_3s_blake2b_0532bf0a1bedbe5a98d1dc449a33ef0c.zip',
    WATERSHED_ID: f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/watersheds_globe_HydroSHEDS_15arcseconds_blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip',
    # Biophysical table:
    'esa_aries_rs3': f'{ECOSHARD_PREFIX}nci-ecoshards/nci-NDR-biophysical_table_ESA_ARIES_RS3_md5_74d69f7e7dc829c52518f46a5a655fb8.csv',
    'nci-ndr-biophysical_table_forestry_grazing': f'{ECOSHARD_PREFIX}nci-ecoshards/nci-NDR-biophysical_table_forestry_grazing_md5_7524f2996fcc929ddc3aaccde249d59f.csv',
    # Precip:
    'worldclim_2015': f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/worldclim_2015_md5_16356b3770460a390de7e761a27dbfa1.tif',
    'worldclim_ssp3': f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/precip_scenarios/he60pr50_md5_829fbd47b8fefb064ae837cbe4d9f4be.tif',
    # LULCs:
    'esacci-lc-l4-lccs-map-300m-p1y-2015-v2.0.7': f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_md5_1254d25f937e6d9bdee5779d377c5aa4.tif',
    'pnv_esa_iis': f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/ESACCI_PNV_iis_OA_ESAclasses_max_ESAresproj_md5_e6575db589abb52c683d44434d428d80.tif',
    'extensification_bmps_irrigated': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_bmps_irrigated_md5_7f5928ea3dcbcc55b0df1d47fbeec312.tif',
    'extensification_bmps_rainfed': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_bmps_rainfed_md5_5350b6acebbff75bb71f27830098989f.tif',
    'extensification_current_practices': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_current_practices_md5_cbe24876a57999e657b885cf58c4981a.tif',
    'extensification_intensified_irrigated': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_intensified_irrigated_md5_215fe051b6bc84d3e15a4d1661b6b936.tif',
    'extensification_intensified_rainfed': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/extensification_intensified_rainfed_md5_47050c834831a6bc4644060fffffb052.tif',
    'fixedarea_bmps_irrigated': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/fixedarea_bmps_irrigated_md5_857517cbef7f21cd50f963b4fc9e7191.tif',
    'fixedarea_bmps_rainfed': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/fixedarea_bmps_rainfed_md5_3b220e236c818a28bd3f2f5eddcc48b0.tif',
    'fixedarea_intensified_irrigated': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/fixedarea_intensified_irrigated_md5_4990faf720ac68f95004635e4a2c3c74.tif',
    'fixedarea_intensified_rainfed': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/fixedarea_intensified_rainfed_md5_98ac886076a35507c962263ee6733581.tif',
    'global_potential_vegetation': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/global_potential_vegetation_md5_61ee1f0ffe1b6eb6f2505845f333cf30.tif',
    # Fertilizer
    'ag_load_2015':f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/ag_load_scenarios/2015_ag_load_md5_4d8ea3cba0f1720afd4a1f2377fb974e.tif',
    'ag_load_ssp3':f'{ECOSHARD_PREFIX}ipbes-ndr-ecoshard-data/ag_load_scenarios/ssp3_2050_ag_load_md5_9fab631dfdae22d12cd92bb1983f9ef1.tif',
    'intensificationnapp_allcrops_irrigated_max_model_and_observednapprevb_bmps': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/IntensificationNapp_allcrops_irrigated_max_Model_and_observedNappRevB_BMPs_md5_ddc000f7ce7c0773039977319bcfcf5d.tif',
    'intensificationnapp_allcrops_rainfed_max_model_and_observednapprevb_bmps': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/IntensificationNapp_allcrops_rainfed_max_Model_and_observedNappRevB_BMPs_md5_fa2684c632ec2d0e0afb455b41b5d2a6.tif',
    'extensificationnapp_allcrops_rainfedfootprint_gapfilled_observednapprevb': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/ExtensificationNapp_allcrops_rainfedfootprint_gapfilled_observedNappRevB_md5_1185e457751b672c67cc8c6bf7016d03.tif',
    'intensificationnapp_allcrops_irrigated_max_model_and_observednapprevb': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/IntensificationNapp_allcrops_irrigated_max_Model_and_observedNappRevB_md5_9331ed220772b21f4a2c81dd7a2d7e10.tif',
    'intensificationnapp_allcrops_rainfed_max_model_and_observednapprevb': f'{ECOSHARD_PREFIX}nci-ecoshards/scenarios050420/IntensificationNapp_allcrops_rainfed_max_Model_and_observedNappRevB_md5_1df3d8463641ffc6b9321e73973f3444.tif',
}

# put IDs here that need to be scrubbed, you may know these a priori or you
# may run the pipeline and see an error and realize you need to add them
SCRUB_IDS = {
    'worldclim_ssp3',
}

# DEFINE SCENARIOS HERE SPECIFYING 'lulc_id', 'precip_id', 'fertilizer_id', and 'biophysical_table_id'
# name the key of the scenario something unique
SCENARIOS = {
    'esa2015_driverssp3': {
        'lulc_id': 'esacci-lc-l4-lccs-map-300m-p1y-2015-v2.0.7',
        'precip_id': 'worldclim_ssp3',
        'fertilizer_id': 'ag_load_ssp3',
        'biophysical_table_id': 'esa_aries_rs3',
    },
    'pnv_driverssp3': {
        'lulc_id': 'pnv_esa_iis',
        'precip_id': 'worldclim_ssp3',
        'fertilizer_id': 'ag_load_ssp3',
        'biophysical_table_id': 'esa_aries_rs3',
    },
}


def _setup_logger(name, log_file, level):
    """Create arbitrary logger to file.

    Args:
        name (str): arbitrary name of logger
        log_file (str): path to file to log to
        level (logging.LEVEL): the log level to report.

    Returns:
        logger object
    """
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


LOGGER = _setup_logger('cbd_global_ndr_plus', 'log.out', level=logging.DEBUG)
PYGEOPROCESSING_LOGGER = _setup_logger('pygeoprocessing', 'pygeoprocessinglog.out', level=logging.INFO)
INSPRING_LOGGER = _setup_logger('inspring', 'inspringlog.out', level=logging.DEBUG)
REPORT_WATERSHED_LOGGER = _setup_logger('report_watershed', 'report_watershed.out', level=logging.DEBUG)
_ = _setup_logger(__name__, 'everythinglog.out', level=logging.DEBUG)

@retrying.retry(
    wait_exponential_multiplier=500, wait_exponential_max=3200,
    stop_max_attempt_number=100)
def _execute_sqlite(
        sqlite_command, database_path, argument_list=None,
        mode='read_only', execute='execute', fetch=None):
    """Execute SQLite command and attempt retries on a failure.

    Args:
        sqlite_command (str): a well formatted SQLite command.
        database_path (str): path to the SQLite database to operate on.
        argument_list (list): ``execute == 'execute'`` then this list is passed
            to the internal sqlite3 ``execute`` call.
        mode (str): must be either 'read_only' or 'modify'.
        execute (str): must be either 'execute' or 'script'.
        fetch (str): if not ``None`` can be either 'all' or 'one'.
            If not None the result of a fetch will be returned by this
            function.

    Returns:
        result of fetch if ``fetch`` is not None.

    """
    cursor = None
    connection = None
    try:
        if mode == 'read_only':
            ro_uri = r'%s?mode=ro' % pathlib.Path(
                os.path.abspath(database_path)).as_uri()
            LOGGER.debug(
                '%s exists: %s', ro_uri, os.path.exists(os.path.abspath(
                    database_path)))
            connection = sqlite3.connect(ro_uri, uri=True)
        elif mode == 'modify':
            connection = sqlite3.connect(database_path)
        else:
            raise ValueError('Unknown mode: %s' % mode)

        if execute == 'execute':
            if argument_list is None:
                cursor = connection.execute(sqlite_command)
            else:
                cursor = connection.execute(sqlite_command, argument_list)
        elif execute == 'script':
            cursor = connection.executescript(sqlite_command)
        elif execute == 'executemany':
            cursor = connection.executemany(sqlite_command, argument_list)
        else:
            raise ValueError('Unknown execute mode: %s' % execute)

        result = None
        payload = None
        if fetch == 'all':
            payload = (cursor.fetchall())
        elif fetch == 'one':
            payload = (cursor.fetchone())
        elif fetch is not None:
            raise ValueError('Unknown fetch mode: %s' % fetch)
        if payload is not None:
            result = list(payload)
        cursor.close()
        connection.commit()
        connection.close()
        cursor = None
        connection = None
        return result
    except sqlite3.OperationalError:
        LOGGER.exception(
            f'{database_path} database is locked because another process is '
            'using it, waiting for a bit of time to try again\n'
            f'{sqlite_command}')
        raise
    except Exception:
        LOGGER.exception('Exception on _execute_sqlite: %s', sqlite_command)
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.commit()
            connection.close()


def _create_work_table_schema(database_path):
    """Create database exists and/or ensures it is compatible and recreate.

    Args:
        database_path (str): path to an existing database or desired
            location of a new database.

    Returns:
        None.

    """
    sql_create_table_script = (
        """
        CREATE TABLE work_status (
            watershed_id TEXT NOT NULL,
            status TEXT NOT NULL,
            PRIMARY KEY (watershed_id)
        );
        CREATE TABLE global_variables (
            watershed_basename TEXT NOT NULL,
            watershed_count INT NOT NULL,
            PRIMARY KEY (watershed_basename)
        );
        """)

    # create the base table
    _execute_sqlite(
        sql_create_table_script, database_path,
        mode='modify', execute='script')


def _set_work_status(database_path, watershed_id_status_list):
    try:
        sql_statement = '''
            INSERT OR REPLACE INTO work_status(watershed_id, status)
            VALUES(?, ?);
        '''
        _execute_sqlite(
            sql_statement, database_path, argument_list=watershed_id_status_list,
            mode='modify', execute='executemany')
    except Exception as e:
        print(f'{e} happened on work status')
        raise


def detect_invalid_values(base_raster_path, rtol=0.001, max_abs=1e30):
    """Raise an exception if an invalid value is found in the raster.

    A ValueError is raised if there are any non-finite values, any values
    that are close to nodata but not equal to nodata, or any values that
    are just really big. If none of these are true then the function returns
    ``True``.
    """
    base_nodata = pygeoprocessing.get_raster_info(
        base_raster_path)['nodata'][0]
    for _, block_array in pygeoprocessing.iterblocks((base_raster_path, 1)):
        non_finite_mask = ~numpy.isfinite(block_array)
        if non_finite_mask.any():
            raise ValueError(
                f'found some non-finite values in {base_raster_path}: '
                f'{block_array[non_finite_mask]}')

        large_value_mask = numpy.abs(block_array) >= max_abs
        if large_value_mask.any():
            raise ValueError(
                f'found some very large values in {base_raster_path}: '
                f'{block_array[large_value_mask]}')

        close_to_nodata_mask = numpy.isclose(
            block_array, base_nodata, rtol=rtol) & (
            block_array != base_nodata)
        if close_to_nodata_mask.any():
            raise ValueError(
                f'found some values that are close to nodata {base_nodata} '
                f'but not equal to '
                f'nodata in {base_raster_path}: '
                f'{block_array[close_to_nodata_mask]}')
    return True


def scrub_raster(
        base_raster_path, target_raster_path, target_nodata=None,
        rtol=0.001, max_abs=1e30):
    """Scrub invalid values from base.

    Will search base raster for difficult values like NaN, +-inf, Very Large
    values that may indicate a roundoff error when being compared to nodata.

    Args:
        base_raster_path (str): path to base raster
        target_raster_path (str): path to raster created by this call with
            invalid values 'scrubbed'.
        target_nodata (numeric): if `None` then the nodata value is copied
            from base, otherwise it is set to this value.
        rtol (float): relative tolerance to use when comparing values with
            nodata. Default is set to 1-3.4e38/float32.min.
        max_abs (float): the maximum absolute value to expect in the raster
            anything larger than this will be set to nodata. Defaults to
            1e30.

    Return:
        None
    """
    LOGGER.debug(f'scrubbing {base_raster_path}')
    if (os.path.exists(target_raster_path) and
            os.path.samefile(base_raster_path, target_raster_path)):
        raise ValueError(
            f'{base_raster_path} and {target_raster_path} are the same file')
    base_raster_info = pygeoprocessing.get_raster_info(base_raster_path)
    base_nodata = base_raster_info['nodata'][0]
    if base_nodata is None and target_nodata is None:
        raise ValueError('value base and target nodata are both None')
    if (base_nodata is not None and
            target_nodata is not None and
            base_nodata != target_nodata):
        raise ValueError(
            f'base raster at {base_raster_path} has a defined nodata '
            f'value of {base_nodata} and also a requested '
            f'target {target_nodata} value')
    if target_nodata is None:
        scrub_nodata = base_nodata
    else:
        scrub_nodata = target_nodata

    non_finite_count = 0
    large_value_count = 0
    close_to_nodata = 0

    def _scrub_op(base_array):
        nonlocal non_finite_count
        nonlocal large_value_count
        nonlocal close_to_nodata
        result = numpy.copy(base_array)
        non_finite_mask = ~numpy.isfinite(result)
        non_finite_count += numpy.count_nonzero(non_finite_mask)
        result[non_finite_mask] = scrub_nodata

        large_value_mask = numpy.abs(result) >= max_abs
        large_value_count += numpy.count_nonzero(large_value_mask)
        result[large_value_mask] = scrub_nodata

        close_to_nodata_mask = numpy.isclose(
            result, scrub_nodata, rtol=rtol) & (
            result != scrub_nodata)
        close_to_nodata += numpy.count_nonzero(close_to_nodata_mask)
        result[close_to_nodata_mask] = scrub_nodata

        return result
    LOGGER.debug(
        f'starting raster_calculator op for scrubbing {base_raster_path}')
    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1)], _scrub_op, target_raster_path,
        base_raster_info['datatype'], scrub_nodata)

    if any([non_finite_count, large_value_count, close_to_nodata]):
        LOGGER.warning(
            f'{base_raster_path} scrubbed these values:\n'
            f'\n\tnon_finite_count: {non_finite_count}'
            f'\n\tlarge_value_count: {large_value_count}'
            f'\n\tclose_to_nodata: {close_to_nodata} '
            f'\n\tto the nodata value of: {scrub_nodata}')
    else:
        LOGGER.info(f'{base_raster_path} is CLEAN')


def create_empty_wgs84_raster(cell_size, nodata, target_path):
    """Create an empty wgs84 raster to cover all the world."""
    n_cols = int(360 // cell_size)
    n_rows = int(180 // cell_size)
    gtiff_driver = gdal.GetDriverByName('GTIFF')
    target_raster = gtiff_driver.Create(
        target_path, n_cols, n_rows, 1, gdal.GDT_Float32,
        options=(
            'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
            'BLOCKXSIZE=256', 'BLOCKYSIZE=256'))

    target_band = target_raster.GetRasterBand(1)
    target_band.SetNoDataValue(nodata)
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)
    target_raster.SetProjection(wgs84_srs.ExportToWkt())
    target_raster.SetGeoTransform(
        [-180, cell_size, 0.0, 90.0, 0.0, -cell_size])
    target_raster = None


def stitch_worker(
        stitch_export_raster_path, stitch_modified_load_raster_path,
        stitch_queue):
    """Take elements from stitch queue and stitch into target."""
    try:
        export_raster_list = []
        modified_load_raster_list = []
        workspace_list = []
        status_update_list = []
        watershed_process_count = collections.defaultdict(int)
        while True:
            payload = stitch_queue.get()
            if payload is not None:
                (export_raster_path, modified_load_raster_path,
                 workspace_dir, watershed_basename, watershed_id) = payload
                watershed_process_count[watershed_basename] += 1
                status_update_list.append((watershed_id, COMPLETE_STATUS))

                export_raster_list.append((export_raster_path, 1))
                modified_load_raster_list.append((modified_load_raster_path, 1))
                workspace_list.append(workspace_dir)

                for path in (export_raster_path, modified_load_raster_path):
                    if not os.path.exists(path):
                        print(f'this path was to stich but does not exist: {path}')

            if len(workspace_list) < 1 and payload is not None:
                continue

            worker_list = []
            for target_stitch_raster_path, raster_list in [
                    (stitch_export_raster_path, export_raster_list),
                    (stitch_modified_load_raster_path,
                     modified_load_raster_list)]:
                stitch_worker = threading.Thread(
                    target=pygeoprocessing.stitch_rasters,
                    args=(
                        raster_list,
                        ['near']*len(raster_list),
                        (target_stitch_raster_path, 1)),
                    kwargs={
                        'overlap_algorithm': 'add',
                        'area_weight_m2_to_wgs84': True})
                stitch_worker.start()
                worker_list.append(stitch_worker)
            for worker in worker_list:
                worker.join()
            for workspace_dir in workspace_list:
                #shutil.rmtree(workspace_dir)
                LOGGER.debug(f'would have removed {workspace_dir} but saving it')

            for watershed_basename, count in watershed_process_count.items():
                current_count = _execute_sqlite(
                    '''SELECT watershed_count
                    FROM global_variables
                    WHERE watershed_basename=?''',
                    WORK_STATUS_DATABASE_PATH, mode='modify',
                    execute='execute', fetch='one',
                    argument_list=[watershed_basename])[0]
                _execute_sqlite(
                    '''UPDATE global_variables
                    SET watershed_count=?
                    WHERE watershed_basename=?''',
                    WORK_STATUS_DATABASE_PATH, mode='modify',
                    execute='execute', fetch='one',
                    argument_list=[current_count-count, watershed_basename])

            export_raster_list = []
            modified_load_raster_list = []
            workspace_list = []
            watershed_process_count = collections.defaultdict(int)

            _set_work_status(
                WORK_STATUS_DATABASE_PATH,
                status_update_list)
            status_update_list = []

            if payload is None:
                break
    except Exception:
        LOGGER.exception('something bad happened on ndr stitcher')
        raise


def _create_watershed_id(watershed_path, watershed_fid):
    """Create unique ID from path and FID."""
    watershed_basename = os.path.basename(os.path.splitext(watershed_path)[0])
    return (watershed_basename, f'{watershed_basename}_{watershed_fid}')


def ndr_plus_and_stitch(
        watershed_path, watershed_fid,
        target_cell_length_m,
        retention_length_m,
        k_val,
        flow_threshold,
        routing_algorithm,
        dem_path,
        lulc_path,
        precip_path,
        custom_load_path,
        eff_n_lucode_map,
        load_n_lucode_map,
        target_export_raster_path,
        target_modified_load_raster_path,
        workspace_dir,
        stitch_queue):
    """Invoke ``inspring.ndr_plus`` with stitch.

    Same parameter list as ``inspring.ndr_plus`` with additional args:

    stitch_queue (queue): places export, load, and workspace path here to
        stitch globally and delete the workspace when complete.

    Return:
        ``None``
    """
    try:
        watershed_basename, watershed_id = _create_watershed_id(watershed_path, watershed_fid)
        LOGGER.debug(f'{watershed_id} about to be run')
        ndr_plus(
            watershed_path, watershed_fid,
            target_cell_length_m,
            retention_length_m,
            k_val,
            flow_threshold,
            routing_algorithm,
            dem_path,
            lulc_path,
            precip_path,
            custom_load_path,
            eff_n_lucode_map,
            load_n_lucode_map,
            target_export_raster_path,
            target_modified_load_raster_path,
            workspace_dir)
        LOGGER.debug(f'{watershed_id} is done')
        _set_work_status(
            WORK_STATUS_DATABASE_PATH,
            [(watershed_id, COMPUTED_STATUS)])
        stitch_queue.put(
            (target_export_raster_path, target_modified_load_raster_path,
             workspace_dir, watershed_basename, watershed_id))
    except Exception:
        LOGGER.exception(
            f'this exception happened on {watershed_path} {watershed_fid} but skipping with no problem')


def load_biophysical_table(biophysical_table_path, lulc_field_id):
    """Dump the biophysical table to two dictionaries indexable by lulc.

    Args:
        biophysical_table_path (str): biophysical table that indexes lulc
            codes to 'eff_n' and 'load_n' values. These value can have
            the field 'use raster' in which case they will be replaced with
            a custom raster layer for the lulc code.
        lulc_field_id (str): this is the name of the field that references
            the lulc id.

    Return:
        A tuple of:
        * eff_n_lucode_map: index lulc to nitrogen efficiency
        * load_n_lucode_map: index lulc to base n load
    """
    biophysical_table = pandas.read_csv(biophysical_table_path)
    # clean up biophysical table
    biophysical_table = biophysical_table.fillna(0)
    biophysical_table.loc[
        biophysical_table['load_n'] == 'use raster', 'load_n'] = (
            USE_AG_LOAD_ID)
    biophysical_table['load_n'] = biophysical_table['load_n'].apply(
        pandas.to_numeric)

    eff_n_lucode_map = dict(
            zip(biophysical_table[lulc_field_id], biophysical_table['eff_n']))
    load_n_lucode_map = dict(
        zip(biophysical_table[lulc_field_id], biophysical_table['load_n']))
    return eff_n_lucode_map, load_n_lucode_map


def unzip(zipfile_path, target_unzip_dir):
    """Unzip zip to target_dir."""
    LOGGER.info(f'unzip {zipfile_path} to {target_unzip_dir}')
    os.makedirs(target_unzip_dir, exist_ok=True)
    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(target_unzip_dir)


def unzip_and_build_dem_vrt(
        zipfile_path, target_unzip_dir, expected_tiles_zip_path,
        target_vrt_path):
    """Build VRT of given tiles.

    Args:
        zipfile_path (str): source zip file to extract.
        target_unzip_dir (str): desired directory in which to extract
            the zipfile.
        expected_tiles_zip_path (str): the expected directory to find the
            geotiff tiles after the zipfile has been extracted to
            ``target_unzip_dir``.
        target_vrt_path (str): path to desired VRT file of those files.

    Return:
        ``None``
    """
    unzip(zipfile_path, target_unzip_dir)
    LOGGER.info('build vrt')
    subprocess.run(
        f'gdalbuildvrt {target_vrt_path} {expected_tiles_zip_path}/*.tif',
        shell=True)
    LOGGER.info(f'all done building {target_vrt_path}')


def _report_watershed_count(base_total):
    try:
        n = 20
        sleep_time = 15.0
        last_n_processed = []
        watersheds_left = base_total
        while True:
            time.sleep(sleep_time)
            sql_statement = 'SELECT * FROM global_variables'
            watershed_basename_count_list = _execute_sqlite(
                sql_statement, WORK_STATUS_DATABASE_PATH,
                mode='read_only', execute='execute', fetch='all')
            current_left = numpy.sum([x[1] for x in watershed_basename_count_list])
            last_n_processed.append(watersheds_left-current_left)
            watersheds_left = current_left
            if len(last_n_processed) > n:
                last_n_processed.pop(0)
            n_processed_per_sec = numpy.mean(last_n_processed) / sleep_time
            if n_processed_per_sec > 0:
                seconds_left = watersheds_left / n_processed_per_sec
            else:
                seconds_left = 99999999999
            hours_left = int(seconds_left // 3600)
            seconds_left -= hours_left * 3600
            minutes_left = int(seconds_left // 60)
            seconds_left -= minutes_left*60
            REPORT_WATERSHED_LOGGER.info(
                f'\n******\ntotal left: {watersheds_left}'+
                '\nwatershed status:\n'+'\n'.join([
                    str(v) for v in watershed_basename_count_list]) +
                f'\ntime left: {hours_left}:{minutes_left:02d}:{seconds_left:04.1f}')

    except Exception:
        REPORT_WATERSHED_LOGGER.exception('something bad happened')


def main():
    """Entry point."""
    LOGGER.debug('starting script')
    os.makedirs(WORKSPACE_DIR, exist_ok=True)
    if not os.path.exists(WORK_STATUS_DATABASE_PATH):
        _create_work_table_schema(WORK_STATUS_DATABASE_PATH)

    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, -1)
    os.makedirs(ECOSHARD_DIR, exist_ok=True)
    ecoshard_path_map = {}
    LOGGER.info('scheduling downloads')
    LOGGER.debug('starting downloads')
    for ecoshard_id, ecoshard_url in ECOSHARDS.items():
        ecoshard_path = os.path.join(
            ECOSHARD_DIR, os.path.basename(ecoshard_url))
        LOGGER.debug(f'download {ecoshard_url}')
        LOGGER.debug(f'dlcode: {urllib.request.urlopen(ecoshard_url).getcode()}')
        download_task = task_graph.add_task(
            func=ecoshard.download_url,
            args=(ecoshard_url, ecoshard_path),
            target_path_list=[ecoshard_path])
        ecoshard_path_map[ecoshard_id] = ecoshard_path
    LOGGER.info('waiting for downloads to finish')
    task_graph.join()

    # global DEM that's used
    task_graph.add_task(
        func=unzip_and_build_dem_vrt,
        args=(
            ecoshard_path_map[DEM_ID], ECOSHARD_DIR, DEM_TILE_DIR,
            DEM_VRT_PATH),
        target_path_list=[DEM_VRT_PATH],
        task_name='build DEM vrt')

    watershed_dir = os.path.join(
        ECOSHARD_DIR, 'watersheds_globe_HydroSHEDS_15arcseconds')
    expected_watershed_path = os.path.join(
        watershed_dir, 'af_bas_15s_beta.shp')

    task_graph.add_task(
        func=unzip,
        args=(ecoshard_path_map[WATERSHED_ID], ECOSHARD_DIR),
        target_path_list=[expected_watershed_path],
        task_name='unzip watersheds')
    LOGGER.debug('waiting for downloads and data to construct')
    task_graph.join()
    invalid_value_task_list = []
    os.makedirs(SCRUB_DIR, exist_ok=True)
    for ecoshard_id_to_scrub in SCRUB_IDS:
        ecoshard_path = ecoshard_path_map[ecoshard_id_to_scrub]
        scrub_path = os.path.join(SCRUB_DIR, os.path.basename(ecoshard_path))
        task_graph.add_task(
            func=scrub_raster,
            args=(ecoshard_path, scrub_path),
            target_path_list=[scrub_path],
            task_name=f'scrub {ecoshard_path}')
        ecoshard_path_map[ecoshard_id] = scrub_path
    LOGGER.debug('wait for scrubbing to end')
    task_graph.join()
    LOGGER.debug('done with downloads, check for invalid rasters')
    for ecoshard_id, ecoshard_path in ecoshard_path_map.items():
        if (pygeoprocessing.get_gis_type(ecoshard_path) ==
                pygeoprocessing.RASTER_TYPE):
            invalid_value_task = task_graph.add_task(
                func=detect_invalid_values,
                args=(ecoshard_path,),
                store_result=True)
            invalid_value_task_list.append(invalid_value_task)
    for invalid_value_task in invalid_value_task_list:
        if invalid_value_task.get() is not True:
            raise ValueError(f'invalid raster at {invalid_value_task}')

    total_watersheds = 0
    for watershed_path in glob.glob(os.path.join(watershed_dir, '*.shp')):
        watershed_basename = os.path.basename(
            os.path.splitext(watershed_path)[0])
        watersheds_to_process_count = 0
        watershed_vector = gdal.OpenEx(watershed_path, gdal.OF_VECTOR)
        watershed_layer = watershed_vector.GetLayer()
        for watershed_feature in watershed_layer:
            if watershed_feature.GetGeometryRef().Area() < AREA_DEG_THRESHOLD:
                continue
            watersheds_to_process_count += 1
        sql_statement = '''
            INSERT OR REPLACE INTO
                global_variables(watershed_basename, watershed_count)
            VALUES(?, ?);
        '''
        _execute_sqlite(
            sql_statement, WORK_STATUS_DATABASE_PATH,
            argument_list=[watershed_basename, watersheds_to_process_count],
            mode='modify', execute='execute')
        total_watersheds += watersheds_to_process_count
        watershed_feature = None
        watershed_layer = None
        watershed_vector = None

    LOGGER.info(f'starting watershed status logger')
    report_watershed_thread = threading.Thread(
        target=_report_watershed_count,
        args=(total_watersheds,))
    report_watershed_thread.daemon = True
    report_watershed_thread.start()

    manager = multiprocessing.Manager()
    stitch_worker_list = []
    stitch_queue_list = []
    target_raster_list = []
    watersheds_scheduled = 0
    for scenario_id, scenario_vars in SCENARIOS.items():
        eff_n_lucode_map, load_n_lucode_map = load_biophysical_table(
            ecoshard_path_map[scenario_vars['biophysical_table_id']],
            BIOPHYSICAL_TABLE_IDS[scenario_vars['biophysical_table_id']])

        stitch_queue = manager.Queue()
        stitch_queue_list.append(stitch_queue)
        target_export_raster_path = os.path.join(
            WORKSPACE_DIR, f'{scenario_id}_{TARGET_CELL_LENGTH_M:.1f}_{ROUTING_ALGORITHM}_export.tif')
        target_modified_load_raster_path = os.path.join(
            WORKSPACE_DIR, f'{scenario_id}_{TARGET_CELL_LENGTH_M:.1f}_{ROUTING_ALGORITHM}_modified_load.tif')

        create_empty_wgs84_raster(
            TARGET_WGS84_LENGTH_DEG, -1, target_export_raster_path)
        create_empty_wgs84_raster(
            TARGET_WGS84_LENGTH_DEG, -1, target_modified_load_raster_path)

        target_raster_list.extend(
            [target_export_raster_path, target_modified_load_raster_path])

        stitch_worker_thread = threading.Thread(
            target=stitch_worker,
            args=(
                target_export_raster_path, target_modified_load_raster_path,
                stitch_queue))

        stitch_worker_thread.start()
        stitch_worker_list.append(stitch_worker_thread)

        for watershed_path in glob.glob(os.path.join(watershed_dir, '*.shp')):
            # TODO: this is for debugging
            if watersheds_scheduled >= 100:
                break
            watershed_vector = gdal.OpenEx(watershed_path, gdal.OF_VECTOR)
            watershed_layer = watershed_vector.GetLayer()
            watershed_basename = os.path.splitext(os.path.basename(watershed_path))[0]
            for watershed_feature in watershed_layer:
                # TODO: this is for debugging
                if watersheds_scheduled >= 100:
                    break
                if watershed_feature.GetGeometryRef().Area() < AREA_DEG_THRESHOLD:
                    continue

                watershed_fid = watershed_feature.GetFID()
                local_workspace_dir = os.path.join(
                    WORKSPACE_DIR, scenario_id,
                    f'{watershed_basename}_{watershed_fid}')
                local_export_raster_path = os.path.join(
                    local_workspace_dir, os.path.basename(target_export_raster_path))
                local_modified_load_raster_path = os.path.join(
                    local_workspace_dir, os.path.basename(target_modified_load_raster_path))
                task_graph.add_task(
                    func=ndr_plus_and_stitch,
                    args=(
                        watershed_path, watershed_fid,
                        TARGET_CELL_LENGTH_M,
                        RETENTION_LENGTH_M,
                        K_VAL,
                        FLOW_THRESHOLD,
                        ROUTING_ALGORITHM,
                        DEM_VRT_PATH,
                        ecoshard_path_map[scenario_vars['lulc_id']],
                        ecoshard_path_map[scenario_vars['precip_id']],
                        ecoshard_path_map[scenario_vars['fertilizer_id']],
                        eff_n_lucode_map,
                        load_n_lucode_map,
                        local_export_raster_path,
                        local_modified_load_raster_path,
                        local_workspace_dir,
                        stitch_queue),
                    task_name=f'{watershed_basename}_{watershed_fid}')
                watersheds_scheduled += 1

    LOGGER.debug(f'there are {watersheds_scheduled} scheduled of {total_watersheds} which is {100*watersheds_scheduled/total_watersheds:.2}% done')
    task_graph.join()
    task_graph.close()
    for stitch_queue in stitch_queue_list:
        stitch_queue.put(None)
    for stitch_worker_thread in stitch_worker_list:
        stitch_worker_thread.join()

    # TODO: build overviews and compress
    build_overview_list = []
    for target_raster in target_raster_list:
        compress_raster_path = os.path.join(
            WORKSPACE_DIR,
            f'compress_overview_{os.path.basename(target_raster)}')
        build_overview_process = multiprocessing.Process(
            target=compress_and_overview,
            args=(target_raster, compress_raster_path))
        build_overview_process.start()
        build_overview_list.append(build_overview_process)
    for process in build_overview_list:
        process.join()


def compress_and_overview(base_raster_path, target_raster_path):
    """Compress and overview base to raster."""
    ecoshard.compress_raster(base_raster_path, target_raster_path)
    ecoshard.build_overviews(target_raster_path)


if __name__ == '__main__':
    main()
