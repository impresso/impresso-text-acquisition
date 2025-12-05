name="impresso_text_prep"
image_version="v4"
image_name=$name:$image_version

echo $image_name
echo $GASPAR_USER_NAME
echo $USER_ID


# username and UID can be found in administrative data
docker buildx build . -t $image_name --platform linux/amd64 --build-arg USER_NAME=$GASPAR_USER_NAME --build-arg USER_ID=$USER_ID

#docker run $image_name pip freeze

docker tag $image_name registry.rcp.epfl.ch/impresso/$image_name

docker push registry.rcp.epfl.ch/impresso/$image_name
