import numpy as np
import bokeh
from bokeh.models.widgets import Dropdown
from bokeh.io import output_file, show
from bokeh.models import Callback
from bokeh.plotting import figure
from bokeh.layouts import layout

output_file("dropdown.html")

# Dummy plot, otherwise the Dropdown button is partially hidden...
x = np.linspace(-2*np.pi,2*np.pi,100)
y = np.sin(x)
p = figure(title="Sin(x)", x_axis_label='x', y_axis_label='y')
p.line(x, y)


callback = Callback(args=None, code="""
        console.log(cb_obj)
        var cm_chosen = cb_obj.get('action');
        alert(cm_chosen);
    """)

menu = [("Item %s"%i, "item_%s"%i) for i in range(10) ]
dropdown = Dropdown(label="Dropdown button %s"%bokeh.__version__, type="success", menu=menu, callback=callback)  

show(layout([dropdown,p]))
