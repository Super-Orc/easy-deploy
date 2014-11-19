#!/bin/bash
rm -rf hosts target project
7za a -tzip -mx=0 -xr\!.* easy-deploy.zip .
