sudo docker rm datasummary_test
sudo docker build -t datasummary_test .
sudo docker run --name datasummary_test datasummary_test
