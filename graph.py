import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
from networkx.drawing.nx_agraph import graphviz_layout

connection = sqlite3.connect('mammom.sqlite')
cursor = connection.cursor()




G = nx.diGraph()
rows = cursor.execute('SELECT Bron, Doel FROM relaties')
G.add_edges_from(rows) 
#nx.draw_networkx_labels(G,G,font_size=16)
nx.spring_layout(G)

#pos = graphviz_layout(G)  # positions for all nodes

# nodes



# labels
#nx.draw_networkx_labels(G, pos, font_size=8, font_family='sans-serif')

nx.draw(G,node_size=5, font_size=6, font_color='blue',k=500)
#plt.show()
plt.savefig("graph2.pdf")

#frida
#gnu paralel