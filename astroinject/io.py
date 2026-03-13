from astropy.table import Table
import gzip

import pandas as pd
import logpool as control

from astropy.table import Table, Column
from astropy.io import fits
import numpy as np

import warnings
warnings.filterwarnings("ignore")

def open_table(table_name, config):
	
	if "format" in config:
		format = config["format"]
	else:
		format = "auto"
	
	if format == "auto":
		format = table_name.split(".")[-1]
	
	if format == "gaia":
		control.info(f"reading table {table_name} with format {format}")
	
		table = Table.read(
			table_name,                # e.g. "gaia_chunk.ecsv.gz"
			format="ascii.ecsv",
			guess=False,               # skip format guessing
			fill_values=[('null', 99), ('nan', 99)],
		)
		return table
	
	if format == "desi_coadd":
		control.info(f"reading DESI coadd file {table_name}")
		table = _read_desi_coadd_as_table(table_name)
		return table
			
	if format == "fits":
		table = Table.read(table_name)
	elif ".parquet" in table_name or format == "parquet":
		try:
			table = Table.read(table_name, format="parquet")
		except Exception as e:
			#control.critical(f"Error reading parquet file, falling back to pandas: {e}")
			table = pd.read_parquet(table_name)
			table = Table.from_pandas(table)
	
 
	elif ".csv" in table_name or format == "csv":
		table = Table.read(table_name, format="csv")
	else:
		raise ValueError(f"Unsupported format: {format}")
	return table



def _read_desi_coadd_as_table(path):
    """
    Read DESI DR1 coadd FITS file and return a Table with:

      - FIBERMAP metadata (with Legacy Surveys flux columns renamed)
      - REDSHIFTS science results (SPECTYPE, Z, etc.)
      - wave_<b/r/z> (object column: same array in each row)
      - flux_<b/r/z>, ivar_<b/r/z> (per-fiber 1D arrays)
    """
    hd = fits.open(path, memmap=True)

    try:
        if "FIBERMAP" not in hd:
            raise RuntimeError(f"DESI file {path} has no FIBERMAP extension.")

        fibermap = Table(hd["FIBERMAP"].data)
        n_fiber = len(fibermap)

        # --- get wavelength grid for each band ---
        def get_wave(hd, band):
            name = f"{band.upper()}_WAVELENGTH"
            if name in hd:
                return np.array(hd[name].data, dtype="float32")

            flux_hdu = f"{band.upper()}_FLUX"
            if flux_hdu not in hd:
                return None
            h = hd[flux_hdu].header
            data = hd[flux_hdu].data
            crval = h.get("CRVAL1")
            cdelt = h.get("CDELT1")
            if crval is None or cdelt is None:
                return None
            npix = data.shape[1]
            pix = np.arange(npix, dtype="float32")
            return (crval + cdelt * pix).astype("float32")

        # --- rename Legacy Surveys flux columns to avoid clashes ---
        rename_map = {
            "FLUX_G": "ls_flux_g",
            "FLUX_R": "ls_flux_r",
            "FLUX_Z": "ls_flux_z",
            "FLUX_W1": "ls_flux_w1",
            "FLUX_W2": "ls_flux_w2",
            "FLUX_IVAR_G": "ls_flux_ivar_g",
            "FLUX_IVAR_R": "ls_flux_ivar_r",
            "FLUX_IVAR_Z": "ls_flux_ivar_z",
            "FLUX_IVAR_W1": "ls_flux_ivar_w1",
            "FLUX_IVAR_W2": "ls_flux_ivar_w2",
            "FIBERFLUX_G": "ls_fiberflux_g",
            "FIBERFLUX_R": "ls_fiberflux_r",
            "FIBERFLUX_Z": "ls_fiberflux_z",
            "FIBERTOTFLUX_G": "ls_fibertotflux_g",
            "FIBERTOTFLUX_R": "ls_fibertotflux_r",
            "FIBERTOTFLUX_Z": "ls_fibertotflux_z",
        }
        for old, new in rename_map.items():
            if old in fibermap.colnames:
                fibermap.rename_column(old, new)

        # --- attach spectral arrays ---
        bands = ("b", "r", "z")
        for band in bands:
            flux_hdu = f"{band.upper()}_FLUX"
            ivar_hdu = f"{band.upper()}_IVAR"

            # flux_<band>
            if flux_hdu in hd:
                flux_2d = np.asarray(hd[flux_hdu].data, dtype="float32")
                fibermap[f"flux_{band}"] = Column(
                    [flux_2d[i, :].copy() for i in range(n_fiber)],
                    name=f"flux_{band}",
                    dtype=object,
                )
            else:
                fibermap[f"flux_{band}"] = Column(
                    [None] * n_fiber, name=f"flux_{band}", dtype=object
                )

            # ivar_<band>
            if ivar_hdu in hd:
                ivar_2d = np.asarray(hd[ivar_hdu].data, dtype="float32")
                fibermap[f"ivar_{band}"] = Column(
                    [ivar_2d[i, :].copy() for i in range(n_fiber)],
                    name=f"ivar_{band}",
                    dtype=object,
                )
            else:
                fibermap[f"ivar_{band}"] = Column(
                    [None] * n_fiber, name=f"ivar_{band}", dtype=object
                )

            # wave_<band> (same array for all fibers)
            wave = get_wave(hd, band)
            fibermap[f"wave_{band}"] = Column(
                [wave] * n_fiber, name=f"wave_{band}", dtype=object
            )

        # --- RA/DEC column standardisation ---
        if "TARGET_RA" in fibermap.colnames:
            fibermap.rename_column("TARGET_RA", "ra")
        if "TARGET_DEC" in fibermap.colnames:
            fibermap.rename_column("TARGET_DEC", "dec")

        # --- merge REDSHIFTS (science classification) ---
        if "REDSHIFTS" in hd:
            red = Table(hd["REDSHIFTS"].data)
            # assume same length and order; copy columns except TARGETID
            if len(red) == len(fibermap):
                for col in red.colnames:
                    if col == "TARGETID":
                        continue
                    if col in fibermap.colnames:
                        # avoid clashes by prefixing, but this should rarely happen
                        new_name = f"red_{col}"
                        fibermap[new_name] = red[col]
                    else:
                        fibermap[col] = red[col]
            else:
                # safer join on TARGETID if lengths differ
                if "TARGETID" in fibermap.colnames and "TARGETID" in red.colnames:
                    fibermap = Table.join(
                        fibermap, red, keys="TARGETID", join_type="left", table_names=["fm", "red"], uniq_col_name="{col}_{table}"
                    )

        # prune to science-relevant columns
        fibermap = prune_desi_columns(fibermap)

        return fibermap

    finally:
        hd.close()


def prune_desi_columns(tbl):
    """
    Keep only science-relevant columns from a DESI DR1 coadd table.
    All purely bookkeeping / ID columns are removed.
    """

    keep = {
        # 1) Sky position & astrometry
        "TARGETID",
        "ra", "dec", "PMRA", "PMDEC", "REF_EPOCH",

        # 2) Legacy Surveys photometry (renamed)
        "ls_flux_g", "ls_flux_r", "ls_flux_z",
        "ls_flux_w1", "ls_flux_w2",
        "ls_flux_ivar_g", "ls_flux_ivar_r", "ls_flux_ivar_z",
        "ls_flux_ivar_w1", "ls_flux_ivar_w2",
        "ls_fiberflux_g", "ls_fiberflux_r", "ls_fiberflux_z",
        "ls_fibertotflux_g", "ls_fibertotflux_r", "ls_fibertotflux_z",

        # 3) DESI coadd spectra
        "flux_b", "ivar_b", "wave_b",
        "flux_r", "ivar_r", "wave_r",
        "flux_z", "ivar_z", "wave_z",

        # 4) Spectroscopic results (from REDSHIFTS)
        "SPECTYPE",      # STAR / GALAXY / QSO
        "Z", "ZERR",
        "ZWARN",
        "DELTACHI2",

        # 5) A bit of useful context
        "OBJTYPE",       # BAD / SKY / TGT (can filter if you want)
        "EBV",
        "COADD_EXPTIME", "COADD_NUMEXP", "COADD_NUMTILE",
        "MEAN_FIBER_RA", "MEAN_FIBER_DEC",
        "MEAN_PSF_TO_FIBER_SPECFLUX",
    }

    keep = [c for c in keep if c in tbl.colnames]
    drop = [c for c in tbl.colnames if c not in keep]
    tbl.remove_columns(drop)

    return tbl