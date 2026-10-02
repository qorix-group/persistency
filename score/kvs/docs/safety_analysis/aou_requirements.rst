..
   # *******************************************************************************
   # Copyright (c) 2026 Contributors to the Eclipse Foundation
   #
   # See the NOTICE file(s) distributed with this work for additional
   # information regarding copyright ownership.
   #
   # This program and the accompanying materials are made available under the
   # terms of the Apache License Version 2.0 which is available at
   # https://www.apache.org/licenses/LICENSE-2.0
   #
   # SPDX-License-Identifier: Apache-2.0
   # *******************************************************************************

KVS Component Assumptions of Use
================================

.. document:: KVS Component AoU
   :id: doc__kvs_comp_aou
   :status: valid
   :version: 1
   :safety: ASIL_B
   :security: YES
   :realizes: wp__requirements_comp_aou[version==1]
   :tags: persistency

This document contains the assumptions of use (AoU) of the KVS component on its user and on its environment.

Assumptions on the User
-----------------------

.. aou_req:: Single Process Access
   :id: aou_req__kvs__single_process
   :reqtype: Process
   :security: NO
   :safety: ASIL_B
   :status: valid
   :version: 1
   :tags: persistency

   The user shall access the storage files of a KVS instance from one process only.

   Note: The component provides thread-safe access within one process (see :need:`comp_req__kvs__concurrency`),
   but no synchronization between processes.

.. aou_req:: Data Versioning by the Application
   :id: aou_req__kvs__data_versioning
   :reqtype: Process
   :security: NO
   :safety: ASIL_B
   :status: valid
   :version: 1
   :tags: persistency

   The application shall implement the versioning and the upgrade of its persisted data structures,
   if it reads data written by a previous version of the application.

   Note: The component does not provide built-in versioning (see :need:`comp_req__kvs__pers_data_version`).

Assumptions on the Environment
------------------------------

.. aou_req:: File System Access Permissions
   :id: aou_req__kvs__fs_permissions
   :reqtype: Process
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :tags: persistency, environment

   The system integrator shall configure the access permissions of the file system for the storage
   location of a KVS instance so that only the authorized application can read and write its storage files.

   Note: The component relies on the file system for access and permission management
   (see :need:`comp_req__kvs__permission_control`).
