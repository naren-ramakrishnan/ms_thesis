#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Andreas Christian Mueller <amueller@ais.uni-bonn.de>
# (c) 2012
#
# License: MIT

from __future__ import division

# further edited by
__author__ = "Aravindan Mahendiran"
__email__ = "aravind@vt.edu"
__processor__ = "wordCloudGenerator"
__version__ = "1.0.0"

from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont
import numpy as np
from query_integral_image import query_integral_image
import random
import os

# fontPath for wordcloud
FONT_PATH = '/usr/share/fonts/truetype/ubuntu-font-family/UbuntuMono-R.ttf'


def make_wordcloud(words, counts, fname, font_path=None, width=1000, height=500,
                   margin=5, ranks_only=False):
    """Build word cloud using word counts, store in image."""

    if len(counts) <= 0:
        print("We need at least 1 word to plot a word cloud, got %d."
              % len(counts))

    if font_path is None:
        font_path = FONT_PATH

    if not os.path.exists(font_path):
        raise ValueError("The provided font %s does not exist." % font_path)

    # normalize counts
    counts = counts / float(counts.max())
    # sort words by counts
    inds = np.argsort(counts)[::-1]
    counts = counts[inds]
    words = words[inds]
    # create image
    img_grey = Image.new("L", (width, height))
    draw = ImageDraw.Draw(img_grey)
    integral = np.zeros((height, width), dtype=np.uint32)
    img_array = np.asarray(img_grey)
    font_sizes, positions, orientations = [], [], []
    # intitiallize font size "large enough"
    font_size = 1000
    # start drawing grey image
    for word, count in zip(words, counts):
        # alternative way to set the font size
        if not ranks_only:
            font_size = min(font_size, int(100 * np.log(count + 100)))
        while True:
            # try to find a position
            font = ImageFont.truetype(font_path, font_size)
            # transpose font optionally
            orientation = random.choice([None, Image.ROTATE_90])
            transposed_font = ImageFont.TransposedFont(font,
                                                       orientation=orientation)
            draw.setfont(transposed_font)
            # get size of resulting text
            box_size = draw.textsize(word)
            # find possible places using integral image:
            result = query_integral_image(integral, box_size[1] + margin,
                                          box_size[0] + margin)
            if result is not None or font_size == 0:
                break
            # if we didn't find a place, make font smaller
            font_size -= 1

        if font_size == 0:
            # we were unable to draw any more
            break

        x, y = np.array(result) + margin // 2
        # actually draw the text
        draw.text((y, x), word, fill="white")
        positions.append((x, y))
        orientations.append(orientation)
        font_sizes.append(font_size)
        # recompute integral image
        img_array = np.asarray(img_grey)
        # recompute bottom right
        # the order of the cumsum's is important for speed ?!
        partial_integral = np.cumsum(np.cumsum(img_array[x:, y:], axis=1),
                                     axis=0)
        # paste recomputed part into old image
        # if x or y is zero it is a bit annoying
        if x > 0:
            if y > 0:
                partial_integral += (integral[x - 1, y:]
                                     - integral[x - 1, y - 1])
            else:
                partial_integral += integral[x - 1, y:]
        if y > 0:
            partial_integral += integral[x:, y - 1][:, np.newaxis]

        integral[x:, y:] = partial_integral

    # redraw in color
    img = Image.new("RGB", (width, height))
    draw = ImageDraw.Draw(img)
    everything = zip(words, font_sizes, positions, orientations)
    for word, font_size, position, orientation in everything:
        font = ImageFont.truetype(font_path, font_size)
        # transpose font optionally
        transposed_font = ImageFont.TransposedFont(font,
                                                   orientation=orientation)
        draw.setfont(transposed_font)
        #draw.text((position[1], position[0]), word, fill="hsl(%d" % random.randint(0, 255) + ", 80%, 50%)")
        draw.text((position[1], position[0]), word)
    #img.show()
    img.save(fname)
