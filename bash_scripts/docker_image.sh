name="impresso_text_prep"
image_version="v1"
image_name=$name:$image_version

echo $image_name

# username and UID can be found in administrative data
sudo docker buildx build . -t $image_name --platform linux/amd64 --build-arg USER_NAME=$GASPAR_USER_NAME --build-arg USER_ID=$USER_ID

sudo docker run $image_name pip freeze

sudo docker tag $image_name ic-registry.epfl.ch/dhlab/$image_name

docker push ic-registry.epfl.ch/dhlab/$image_name
