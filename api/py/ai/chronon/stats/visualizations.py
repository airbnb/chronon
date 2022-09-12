"""
Visualization utils.
"""
import matplotlib.pyplot as plt
import numpy as np


def plot_histogram(featureTable, column, cap, ds, bins = 51, lookback = 30):
    histogram = extract_histogram(ds, featureTable, column, cap, ds, lookback, bins)
    bins = [0]
    counts = []
    for b, c in sorted(histogram.items(), key=lambda x: float(x[0])):
      bins.append(float(b))
      counts.append(c)
    bins = np.array(bins).astype(float)
    counts = np.array(counts).astype(float)
    centroids = (bins[1:] + bins[:-1]) / 2
    counts_, bins_, _ = plt.hist(centroids, bins=len(counts),
                                 weights=counts, range=(min(bins), max(bins)))
    plt.title(f"Histogram for {column}\n (capped at {cap}, ranging from {first_ds} to {ds})")
    plt.show()


# Heatmap utils from https://matplotlib.org/stable/gallery/images_contours_and_fields/image_annotated_heatmap.html
def heatmap(data, row_labels, col_labels, title="Heatmap", ax=None,
            cbar_kw={}, cbarlabel="", **kwargs):
    """
    Create a heatmap from a numpy array and two lists of labels.

    Parameters
    ----------
    data
        A 2D numpy array of shape (M, N).
    row_labels
        A list or array of length M with the labels for the rows.
    col_labels
        A list or array of length N with the labels for the columns.
    ax
        A `matplotlib.axes.Axes` instance to which the heatmap is plotted.  If
        not provided, use current axes or create a new one.  Optional.
    cbar_kw
        A dictionary with arguments to `matplotlib.Figure.colorbar`.  Optional.
    cbarlabel
        The label for the colorbar.  Optional.
    **kwargs
        All other arguments are forwarded to `imshow`.
    """
    if not ax:
        _, ax = plt.subplots(1, 1, figsize=(12, 26))

    # Show all ticks and label them with the respective list entries.
    ax.set_xticks(np.arange(data.shape[1]), labels=col_labels)
    ax.set_yticks(np.arange(data.shape[0]), labels=row_labels)
    ax.locator_params(axis='x', nbins=10)

    # Let the horizontal axes labeling appear on top.
    ax.tick_params(top=True, bottom=False,
                   labeltop=True, labelbottom=False)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=-30, ha="right",
             rotation_mode="anchor")

    # Turn spines off and create white grid.
    ax.spines[:].set_visible(False)

    ax.set_xticks(np.arange(0, data.shape[1], 2)-0.5, minor=True)
    ax.set_yticks(np.arange(0, data.shape[0])-0.25, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=0)
    ax.tick_params(which="minor", bottom=False, left=False, labelsize=4)
    im = ax.imshow(data, **kwargs)
    plt.title(title)
    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")
    return im, cbar


def prefixed_heatmap(df, prefix, **kwargs):
    filtered = df.filter(regex=f"^{prefix}").rename(columns=lambda x: x[len(prefix):].replace('_', ' '))
    y_labels = filtered.columns.to_numpy()
    x_labels = df["ds"].to_numpy()
    dataframe = filtered.transpose().to_numpy()
    heatmap(dataframe, y_labels, x_labels, title=prefix, cmap='YlOrRd', cbar_kw={"shrink": 0.2}, **kwargs)
