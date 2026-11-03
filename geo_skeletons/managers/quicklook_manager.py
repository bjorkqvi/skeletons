from ..errors import ProjectionError
from .. import logger
from typing import Optional
import numpy as np
from pyproj import Transformer, CRS

COMPARE_COLORS = ['k','r','m','c','g','y']
COMPARE_MARKERS = ['.','x','*','s','h','o']

MAX_BINS = 25
MIN_BINS = 5

def determine_levels(vlim):
    """Determines plotting levels
    
    Allowed binds are (and preferred in this order)
    a) 1 
    b) multiples of 10
    c) Multiples of 5
    d) Multiples of 0.25
    e) 0.1, 0.01, ... etc.
    """
    vmin, vmax = vlim
    
    # Start out by trying bin size 1
    l0, l1 = np.floor(vmin), np.ceil(vmax)
    res = 1
    nbins = (l1-l0)/res

    while nbins > MAX_BINS or nbins < MIN_BINS:
        res = (l1-l0)/nbins
        if nbins > MIN_BINS*10:
            res = res*10
        elif res>= 0.5:
            res = res/2
        elif res > 0.1:
            res = 0.1
        else:
            res = res/10
            
        l0, l1 =np.floor(vmin/res)*res, np.ceil(vmax/res)*res
        nbins = (l1-l0)/res

 
    levels = np.arange(l0, l1+res, res)

    return levels


class QuickLookManager:
    def __init__(self, skeleton):
        self.skeleton = skeleton

    def plot_compare_grids(self, ax, compare, proj, compare_colors: list[str]=None, compare_markers: list[str]=None, compare_sizes: list[float]=None, compare_labels: list[str] = None):
        """Plots points for grids that we want to compare to"""
        if compare is None:
            return ax

        if not isinstance(compare, list):
            compare = [compare]

        compare_colors = compare_colors or COMPARE_COLORS
        compare_markers = compare_markers or COMPARE_MARKERS
        compare_sizes = compare_sizes or [5]*len(compare)
        compare_labels = compare_labels or [c.name for c in compare]
        if len(compare) > len(compare_colors):
            raise ValueError(f"Can't compare {len(compare)} skeletons since I only have {len(compare_colors)} defined colors!")

        
        
        for n, comp in enumerate(compare):
            if proj == 'crs' or proj is None and self.skeleton.core.is_projected():
                x, y = comp.xy(crs=self.proj.crs())
            else:
                x, y = comp.lonlat()
            
            ax.scatter(x, y,c=compare_colors[n],s=compare_sizes[n], marker=compare_markers[n],label=compare_labels[n])
        return ax


    def ql_main(self, proj, compare: "Skeleton", contour: bool , mag: bool, dir: bool, arrows: bool, arrow_vars: dict[str, str], wrt: Optional[str], sparse: bool, show: bool, coastline: bool, res: str, compare_colors: list[str], compare_markers: list[str], compare_sizes: list[float], compare_labels:list[str]):
        try:
            import matplotlib.pyplot as plt
        except ImportError as e:
            logger.log.info(f"Quicklook required matplotlib")
            raise e

        arrow_vars = arrow_vars or {}
        vars = self.determine_quicklook_variables(mag, dir, arrows,arrow_vars)
        
        # No data to plot: only plot points
        if not vars:
            fig, ax = plt.subplots(1, 1)
            if proj is None:
                x, y = self.skeleton.xy(native=True)
            elif proj =='lonlat':
                x, y = self.skeleton.lonlat()
            elif proj == 'crs':
                x, y = self.skeleton.xy()
            ax[1,1].scatter(x,y)
            ax[1,1] = self.plot_compare_grids(ax[1,1], compare, proj, compare_colors, compare_markers, compare_sizes, compare_labels)
            ax[1,1].legend(loc='upper right')
            if show:
                plt.show()
            return

        # Init plot
        cols = int(np.ceil(len(vars)**0.5))
        rows = int(np.ceil(len(vars)/cols))
        fig, ax = plt.subplots(rows, cols)
        ax = np.atleast_2d(ax)
        
        if wrt not in ['crs', 'lonlat', None]:
            raise ValueError(f"Invalid value for 'wrt': {wrt}. Must be one of 'crs', 'lonlat', or None.")

        
        r, c = 0, 0
        for var, var_dict in vars.items():
            
            # Get data in rigth CRS and create corresponding string
            try:
                data = self.skeleton.get(var, wrt=wrt, squeeze=False)
                if wrt == 'crs':
                    wrt_str = ' with respect to CRS'
                elif wrt == 'lonlat':
                    wrt_str = ''
                elif self.skeleton.core.values_are_projected(var):
                    wrt_str = ' with respect to CRS'
                else:
                    wrt_str = ''
            except ProjectionError: # Data can't be rotated
                data = self.skeleton.get(var, wrt=None, squeeze=False)
                wrt = ''
            
            
            if not var_dict.get('is_arrow', False): # Normal data that will not be plotted as arrows
                if 'time' in self.skeleton.core.coords():
                    data = data[0,...]
                        
                # Set possible limits if plotting direction as magnitude
                cmap = var_dict.get('cmap','viridis') 
                if cmap == 'twilight':
                    if self.skeleton.core.meta_parameter(var).dir_type() in  ['to', 'from']:
                        vlim = (0, 360)
                    else:
                        vlim = (-np.pi, np.pi)
                else:
                    vlim = (np.nanmin(data), np.nanmax(data))

                if self.skeleton.is_gridded():
                    ax[r,c], cont = self.plot_gridded_data(ax[r,c], data, proj, contour, cmap, vlim=vlim, res=res)
                else:
                    ax[r,c], cont = self.plot_point_data(ax[r,c], data, proj, contour, cmap, vlim=vlim, res=res)

            # See if we need to plot arrow data. Can be either the data variable, or some other data that will be plotted on top of the data variable
            arrow_var = var_dict.get('arrow_data', '')
            if var_dict.get('is_arrow', False):
                arrow_data = self.skeleton.get(var, dir_type='math', wrt=wrt, squeeze=False)
            elif arrow_var:
                arrow_data = self.skeleton.get(arrow_var, dir_type='math', wrt=wrt, squeeze=False)
            else:
                arrow_data = None

            if arrow_data is not None:
                arrow_data = self.skeleton.get(var, dir_type='math', wrt=wrt, squeeze=False)
                if 'time' in self.skeleton.core.coords():
                    arrow_data = arrow_data[0,...]
                if self.skeleton.is_gridded():
                    ax[r,c] = self.quiver_gridded(ax[r,c], arrow_data, proj, arrow_var, wrt, sparse) 
                else:
                    ax[r,c] = self.quiver_point(ax[r,c], arrow_data, proj, arrow_var, wrt, sparse) 
                cont = None
                
                wrt = ''

            # Add colorbar if necessary
            if cont is not None:
                cbar = fig.colorbar(cont, ax=ax[r, c])
                param = self.skeleton.core.meta_parameter(var)
                if param is not None:
                    units = param.units()
                    if param.dir_type() == 'to':
                        units += ' to'
                    elif param.dir_type() == 'from':
                        units += ' from'
                else:
                    units = '?'
                cbar.set_label(f"{var} [{units}]{wrt_str}")

            ax[r,c] = self.plot_compare_grids(ax[r,c], compare, proj, compare_colors, compare_markers, compare_sizes, compare_labels)
            
            if coastline:
                ax[r,c] = self.add_coastline(ax[r,c], proj)

            # Add titles, labels etc.
            if proj is None:
                ax[r,c].set_xlabel(self.skeleton.core.x_str)
                ax[r,c].set_ylabel(self.skeleton.core.y_str)
            elif proj == 'lonlat':
                ax[r,c].set_xlabel('longitude')
                ax[r,c].set_ylabel('latitude')
            elif proj == 'crs':
                ax[r,c].set_xlabel('x')
                ax[r,c].set_ylabel('y')

            title_str = f"{self.skeleton.name}"
            if 'time' in self.skeleton.core.coords():
                title_str += f": {self.skeleton.time(datetime=False)[0]}"
            ax[r,c].set_title(title_str)
            
            ax[r,c].legend(loc='upper right')
            
            c += 1
            if c > cols-1:
                c = 0
                r += 1


        if show:
            plt.show()


    def plot_gridded_data(self, ax, data: np.ndarray, proj: str, contour: bool, cmap: str, vlim: tuple[float], res: str):
        """This is called by the quicklook method of the Skelton class
        
        res [str]: 'low', 'med', 'high' or 'auto' (default)"""
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors

        levels = determine_levels(vlim)

        if len(levels) < 2 and contour:
            logger.log.info(f'Need at least two levels to use contour. Setting to False.')
            contour = False
        if len(data.shape) == 1 and contour:
            logger.log.info(f'Need true 2D-data (not {data.shape}) to use contour. Setting to False.')
            contour=False

        cmap = plt.get_cmap(cmap, len(levels)-1)
        
        norm = mcolors.BoundaryNorm(boundaries=levels, ncolors=cmap.N, clip=True)

        if res == 'auto':        
            if self.skeleton.nx() < 200 and self.skeleton.ny() < 200:
                res = 'high'
            else:
                res = 'med'

        
        if not contour and len(data.shape) > 1 and res in ['low','med']:
            native_proj = False
            if proj is None:
                native_proj = True
            elif proj == 'lonlat' and not self.skeleton.core.is_projected():
                native_proj = True
            elif proj == 'xy' and self.skeleton.core.is_projected():
                native_proj = True
            
            if native_proj: # Reprojected data is not longer gridded
                xe = self.skeleton.edges('x', native=True)
                ye = self.skeleton.edges('y', native=True)
                
                
                if res == 'low':
                    stepx = int(self.skeleton.nx()/200)
                    stepy = int(self.skeleton.ny()/200)
                else:
                    stepx, stepy = (1, 1)
                cont = ax.imshow(data[::stepy, ::stepx], cmap=cmap,norm=norm, extent=(xe[0], xe[1], ye[1], ye[0]), interpolation='nearest')
            return ax, cont
        
        if proj is None:
            x, y = self.skeleton.x(native=True), self.skeleton.y(native=True)
        elif proj == 'lonlat':
            x, y = self.skeleton.longrid(), self.skeleton.latgrid()
        elif proj == 'xy':
            x, y = self.skeleton.xgrid(), self.skeleton.ygrid()

        if contour:
            cont = ax.contourf(x,y,data, cmap=cmap, levels=levels, norm=norm)
        elif len(data.shape) == 1:
            cont = ax.scatter(x,y, c=data, s=10, cmap=cmap, norm=norm)
        else:
            cont = ax.pcolormesh(x, y, data, shading='auto',cmap=cmap,  norm=norm)

        return ax, cont
    

    def plot_point_data(self, ax, data: np.ndarray, proj: str, contour: bool, cmap: str, vlim: tuple[float], res:str):
        """Plotting data from PointSkeletons
        
        res [str]: 'low', 'med', 'high' or 'auto' (default)"""
        levels = determine_levels(vlim)

        if len(levels) < 2 and contour:
            logger.log.info(f'Need at least two levels to use contour. Setting to False.')
            contour = False

        if proj is None:
            x, y = self.skeleton.xy(native=True)
        elif proj == 'lonlat':
            x, y = self.skeleton.lonlat()
        elif proj == 'xy':
            x, y = self.skeleton.xy()

        mask = np.logical_not(np.isnan(data))
        if contour and sum(mask)<3:
            contour = False

        if res == 'auto':
            if len(data) > 100_000:
                res = 'low'
            elif len(data) > 40_000:
                res = 'med'
            else:
                res = 'high'

        if contour:
            cont = ax.tricontourf(x[mask], y[mask],data[mask], levels=levels, cmap=cmap)
        else:
            try:
                import matplotlib.colors as mcolors
                import matplotlib.pyplot as plt
            except ImportError as e:
                logger.log.info(f"Quicklook required matplotlib")
                raise e
            
            if cmap == 'viridis':
                cmap = plt.cm.viridis
            else:
                cmap = plt.cm.twilight

            norm = mcolors.BoundaryNorm(boundaries=levels, ncolors=cmap.N, clip=True)
            if res in ['low', 'med']:
                if res == 'low':
                    gridsize = 100
                else:
                    gridsize = 300
                cont = plt.hexbin(x[mask], y[mask], C=data[mask], gridsize=gridsize, reduce_C_function=np.mean, cmap=cmap, norm=norm)
            else:
                import matplotlib.tri as mtri
                tri = mtri.Triangulation(x, y)        
                cont = plt.tripcolor(tri, data, shading='gouraud', cmap=cmap, norm=norm) 
            
                #cont = ax.scatter(x, y,c=data, s=2,  cmap=cmap, norm=norm, rasterized=True)
        

        return ax, cont

    def quiver_gridded(self,ax, arrow_data, proj, var, wrt, sparse):
        """Plots quiver arrows from gridded data"""
        if proj is None:
            xgrid, ygrid = self.skeleton.xgrid(native=True), self.skeleton.ygrid(native=True)
        elif proj == 'lonlat':
            xgrid, ygrid = self.skeleton.longrid(), self.skeleton.latgrid()
        elif proj == 'xy':
            xgrid, ygrid = self.skeleton.xgrid(), self.skeleton.ygrid()
        
        
        if sparse:
            if isinstance(sparse, bool):
                nr_of_points = 25
            else:
                nr_of_points = sparse
            if len(arrow_data.shape) == 1:
                step = np.floor(len(arrow_data)/nr_of_points).astype(int)
                step = np.maximum(step, 1)
                arrow_data = arrow_data[::step]
                xgrid = xgrid[::step]
                ygrid = ygrid[::step]
            else:
                xstep = np.floor(arrow_data.shape[1]/nr_of_points).astype(int)
                ystep = np.floor(arrow_data.shape[0]/nr_of_points).astype(int)
                xstep = np.maximum(xstep, 1)
                ystep = np.maximum(ystep, 1)
                arrow_data = arrow_data[::ystep, ::xstep]

                xgrid = xgrid[::ystep, ::xstep]
                ygrid = ygrid[::ystep, ::xstep]

        ax.quiver(xgrid, ygrid, np.cos(arrow_data), np.sin(arrow_data), label=f"{var}{wrt}")


        return ax


    def quiver_point(self,ax, arrow_data, proj, var, wrt, sparse):
        """Plots quiver arrows from point data"""
        if proj is None:
            xgrid, ygrid = self.skeleton.xgrid(native=True), self.skeleton.ygrid(native=True)
        elif proj == 'lonlat':
            xgrid, ygrid = self.skeleton.longrid(), self.skeleton.latgrid()
        elif proj == 'xy':
            xgrid, ygrid = self.skeleton.xgrid(), self.skeleton.ygrid()
        if sparse:
            if isinstance(sparse, bool):
                nr_of_points = 500
            else:
                nr_of_points = sparse
            if len(arrow_data.shape) == 1:
                step = np.floor(len(arrow_data)/nr_of_points).astype(int)
                step = np.maximum(step, 1)
                arrow_data = arrow_data[::step]
                xgrid = xgrid[::step]
                ygrid = ygrid[::step]
        ax.quiver(xgrid, ygrid, np.cos(arrow_data), np.sin(arrow_data), label=f"{var}{wrt}")
        return ax


    def add_coastline(self, ax, proj):
        """Adds a coastline to the plot. Requires that shapely and cartopy are installed"""
        try:
            from shapely.ops import transform
        except ImportError as e:
            logger.log.info(f'Adding a coastline requires shapely')
            raise e

        try:
            import cartopy.feature as cfeature 
        except ImportError as e:
            logger.log.info(f'Adding a coastline requires cartopy')
            raise e


        # Determine to which CRS the coastline needs to be projected
        if proj is None and self.skeleton.core.is_projected():
            crs = self.proj.crs()

        elif proj == "xy":
            crs = self.proj.crs()
        else:
            crs = "EPSG:4326"

        if isinstance(crs, tuple):
            crs = crs_from_utm(crs)
        
        transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)

        coast_feat = cfeature.NaturalEarthFeature('physical', 'coastline', '10m')
        geoms = list(coast_feat.geometries())

        for geom in geoms:
            proj_geom = transform(lambda x, y, z=None: transformer.transform(x, y), geom)
            # proj_geom can be LineString, MultiLineString, Polygon, MultiPolygon
            # handle common types:
            if proj_geom.geom_type == 'LineString':
                x, y = proj_geom.xy
                ax.plot(x, y, color='black', linewidth=0.5)
            elif proj_geom.geom_type == 'MultiLineString':
                for part in proj_geom.geoms:
                    x, y = part.xy
                    ax.plot(x, y, color='black', linewidth=0.5)
            elif proj_geom.geom_type == 'Polygon':
                x, y = proj_geom.exterior.xy
                ax.plot(x, y, color='black', linewidth=0.5)
            elif proj_geom.geom_type == 'MultiPolygon':
                for poly in proj_geom.geoms:
                    x, y = poly.exterior.xy
                    ax.plot(x, y, color='black', linewidth=0.5)


        if proj is None:
            ax.set_xlim(self.skeleton.edges('lon', native=True))
            ax.set_ylim(self.skeleton.edges('lat', native=True))
        elif proj == 'lonlat':
            ax.set_xlim(self.skeleton.edges('lon'))
            ax.set_ylim(self.skeleton.edges('lat'))
        elif proj == 'xy':
            ax.set_xlim(self.skeleton.edges('x'))
            ax.set_ylim(self.skeleton.edges('y'))

        return ax
    
    def determine_quicklook_variables(self, mag: bool, dir: bool, arrows: bool, arrow_vars: dict[str, str]) -> dict[str, dict]:
        """Determines which variables to plot and how to plot them (arrows etc)
        
        If mag = dir = arrows = False:
        Plot only normal data variables (components in case of e.g. wind)

        If mag = True:
        Plot only magnitudes
            If arrows = True:
                Plot quiver plot of direction on top of magnitude
        If dir = True:
        Plot directions as magnitudes with circular colormap

        If arrows = True:
        Plot quiver plots of directions
        """
        vars: dict[str, dict] = {}
        if dir and arrows:
            mag = True
        
        if mag or dir or arrows:
            for var in self.skeleton.core.magnitudes(): # Every direction is connected to a magnitude
                if self.skeleton.get(var, strict=True) is not None:
                    if mag:
                        vars[var] = {}
                    dirparam = self.skeleton.core.get(var).direction
                    if dirparam is not None:
                        if dir:
                            vars[dirparam.name] = {'cmap': 'twilight'}
                        if arrows and mag:
                            vars[var] = {'arrow_data': dirparam.name}
                        elif arrows:
                            vars[dirparam.name] = {'is_arrow': True}
    
        for var in self.skeleton.core.data_vars():
            # Don't plot components if magnitudes or directions are being plotted
            if (mag or dir) and self.skeleton.core.is_component(var):
                continue
            if self.skeleton.get(var, strict=True) is not None:
                vars[var] = {}
                if self.skeleton.core.get(var).dir_type is not None:
                    vars[var] = {'cmap': 'twilight'}
                if arrow_vars.get(var) is not None:
                    vars[var] = {'arrow_data': arrow_vars.get(var)}
        
       
        return vars
    


    
def crs_from_utm(utm: tuple[int, str]) -> CRS:
    """Return a pyproj CRS for UTM zone (1..60) and band letter (C..X).
    Uses hemisphere implied by band (C..M => southern, N..X => northern).
    """

    zone, band = utm
    if not (1 <= zone <= 60):
        raise ValueError("Zone must be 1..60")
    band = band.upper()
    BAND_LETTERS = "CDEFGHJKLMNPQRSTUVWX"
    if band not in BAND_LETTERS:
        raise ValueError("Band must be one of " + ", ".join(BAND_LETTERS))
    hemisphere = "N" if BAND_LETTERS.index(band) >= BAND_LETTERS.index("N") else "S"
    epsg_base = 32600 if hemisphere == "N" else 32700
    epsg = epsg_base + zone
    return CRS.from_epsg(epsg)