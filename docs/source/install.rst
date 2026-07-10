Quick Installation
=============================================
To get started with geo-skeletons, you can install it with pip or conda:


1. Using pip or conda (recommended for normal use)
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. code-block:: shell

   $ pip install geo-skeletons

.. code-block:: shell

   $ conda install -c conda-forge geo-skeletons

2. Cloning the GitHub repository (for development)
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
If you want to work on the code (or use some other branch than main), you can pull take the original code:

1. Pull the code from GihHub

.. code-block:: bash

   $ git clone https://https://github.com/bjorkqvi/skeletons.git
   $ cd skeletons/

2. Create an environment with the required dependencies and install geo-skeletons as an editable package

.. code-block:: bash

  $ conda config --add channels conda-forge
  $ conda env create -f environment.yml
  $ conda activate skeletons
  $ pip install -e .
  