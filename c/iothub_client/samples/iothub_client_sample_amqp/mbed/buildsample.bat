@REM  Copyright (c) Microsoft. All rights reserved.
@REM  Licensed under the MIT license. See LICENSE file in the project root for full license information.


@setlocal
@echo off

for %%i in ("%~dp0..\..\..\..") do set "root_path=%%~fi"

set base=iothub_client_sample_amqp

set "sample_path=%~dp0"
set "zip_path=%~dp0%base%.zip"

call %root_path%\tools\mbed_build\mkmbedzip.cmd "%sample_path%" "%zip_path%" *http*.* *mqtt*.* wolfssl*.*
if %errorlevel% neq 0 goto :eof

call %root_path%\tools\mbed_build\buildsample.cmd "%sample_path%" "%zip_path%" %base%_bld %base% FRDM-K64F
