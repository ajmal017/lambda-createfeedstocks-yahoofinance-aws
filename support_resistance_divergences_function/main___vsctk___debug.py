
# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import ptvsd
import sys
from main import lambda_handler as _handler


def lambda_handler(event, context):
    ptvsd.enable_attach(address=('0.0.0.0', 5858), redirect_output=False)
    print('Waiting for debugger to attach...')
    sys.stdout.flush()
    ptvsd.wait_for_attach()
    print('...debugger attached')
    sys.stdout.flush()
    return _handler(event, context)

