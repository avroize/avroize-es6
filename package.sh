#!/usr/bin/env bash

./lint.sh
./test.sh

./node_modules/.bin/babel src --presets babel-preset-es2015 --out-dir dist