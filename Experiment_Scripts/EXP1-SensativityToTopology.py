from graph_tool.all import *
import os, csv
import GFP
import NetSimilie
from pylab import *
import networkx as nx

# Code for the experiment showing the sensativity to network topology
def printDegreeDist(g, name):
    # Plot the degree distribution of the passed graph
    total_hist = vertex_hist(g, "total")
    y = total_hist[0]
    figure(figsize=(6,4))
    errorbar(total_hist[1][:-1], total_hist[0], fmt="o", label="total")
    gca().set_yscale("log")
    gca().set_xscale("log")
    subplots_adjust(left=0.2, bottom=0.2)
    xlabel("$k_{total}$" , fontsize=25)
    ylabel("$NP(k_{total})$" , fontsize=25)
    tight_layout()
    savefig(name)

def randomRewrite(tempG, statModel, iterations):
    # Method to rewire the graph based on some probabaltic methods.
    # Does not increase or decrease the number of vertices or edges.
    # Will rewire the graph in place
    #https://graph-tool.skewed.de/static/doc/generation.html#graph_tool.generation.random_rewire

    print random_rewire(tempG, model = statModel, n_iter = iterations, edge_sweep = False)
    return tempG

if __name__ == "__main__":
    g = Graph()
    edges = []
    with open("FFSource.graph") as networkData:
        datareader = csv.reader(networkData, delimiter="	")
        for row in datareader:
            if "#" not in row:
                edges.append([int(row[0]), int(row[1])])
    networkData.close()
    g.add_edge_list(edges, hashed=True) # Very important to hash the values here otherwise it creates too many nodes
    g.set_directed(False)
    g1 = Graph(g)

    rewireITR = [100, 1000, 10000, 100000, 1000000, 10000000]
    graphNames = ["1.pdf", "2.pdf", "3.pdf", "4.pdf", "5.pdf", "6.pdf"]
    printDegreeDist(g1, "org.pdf")
    count = 0
    graphs = []
    g.save("org.gml")
    g_nx = nx.read_gml("org.gml", label="id")
    nx.write_edgelist(g_nx, "org.edgelist", data=False, delimiter="    ", encoding="utf-8")

    for i in rewireITR:
        g2 = randomRewrite(g1, "erdos", i)
        graphs.append(g2)
        printDegreeDist(g2, graphNames[count])
        g2.save(str(i)+".gml")
        g_nx = nx.read_gml(str(i)+".gml", label="id")
        nx.write_edgelist(g_nx, str(i)+".edgelist", data=False, delimiter="    ", encoding="utf-8")
        count = count + 1
        g1 = Graph(g)

    count = 0
    # Loop through the generated graphs and compare with the source G
    for gp in graphs:
        nsr = NetSimilie.netSimileControl(g, gp)
        print("NS Result = ", nsr)
