# Testing the install script

## On the local system

Run the install script from the command-line

    $ ./setup/install.sh -d $HOME/gapipe

## In a docker container

Create a new image and run the container

    # docker build -f ./setup/Dockerfile -t gapipe_test . && docker run -it gapipe_test

