..
   # *******************************************************************************
   # Copyright (c) 2025 Contributors to the Eclipse Foundation
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

KVS Detailed Design Description
===============================

.. document:: KVS Detailed Design
   :id: doc__kvs_detailed_design
   :status: valid
   :version: 1
   :safety: ASIL_B
   :security: NO
   :realizes: wp__sw_implementation[version==1]
   :tags: persistency

Description
-----------

The KVS component (:need:`comp__persistency_kvs`) is implemented in Rust and in C++ with the same API concept.
The architecture of the component is described in :need:`doc__kvs_component_architecture`.

Design decisions:

- An instance is created with a builder (``KvsBuilder``), which defines the instance ID and whether persisted data
  and default values are required, optional or ignored.
- All values of an instance are held in memory. Persisted data is loaded on open and written on explicit flush.
- Each instance is protected by one mutex for thread-safe access (:need:`comp_req__kvs__concurrency`).
- The data is stored as JSON data file with an Adler-32 hash file, snapshots are stored as additional file pairs.
- Rust: the storage access is encapsulated in the ``KvsBackend`` trait. The default backend ``JsonBackend``
  uses the crates ``tinyjson`` and ``adler32`` and the Rust standard library file API.
- C++: the file system, JSON parser and writer and logger of the S-CORE baselibs are injected into ``Kvs``
  as interfaces, which allows testing with mocks.

Design constraints:

- Rust: at most ``KVS_MAX_INSTANCES`` (10) instances exist per process; ``KvsBuilder`` returns the existing
  instance when an instance ID is opened again.
- C++: the API functions do not wait for the mutex and return ``MutexLockFailed`` if the instance is locked.

Rationale Behind Decomposition into Units
*****************************************

The units follow the single responsibility principle: API definition, implementation of the API, instance
creation, value representation, storage backend, serialization and error handling are separate units. The
separation of the storage backend (Rust) and the injection of the baselibs interfaces (C++) allow testing the
KVS logic independent of the file system.

Static Diagrams for Unit Interactions
-------------------------------------

Rust implementation:

.. uml:: class_diagram.puml

C++ implementation:

.. uml:: kvs_cpp_static_view.puml

Dynamic Diagrams for Unit Interactions
--------------------------------------

The interactions of the component with its user and the used interfaces are described in the dynamic views of
the feature architecture (:need:`doc__persistency_kvs_architecture`). Within the component the interactions
between the units are direct function calls in the same order, so no additional dynamic diagram is needed.

Units within the Component
--------------------------

Rust implementation (``score/kvs/rust_kvs``):

- ``kvs_api.rs``: API definition (``KvsApi`` trait, ``InstanceId``, ``SnapshotId``, ``KvsDefaults``, ``KvsLoad``)
- ``kvs.rs``: implementation of the API (``Kvs``)
- ``kvs_builder.rs``: creation of instances and instance pool (``KvsBuilder``)
- ``kvs_value.rs``: value representation (``KvsValue``)
- ``kvs_serialize.rs``: conversion of custom types to and from ``KvsValue``
- ``kvs_backend.rs``: storage backend interface (``KvsBackend``)
- ``json_backend.rs``: JSON storage backend with hash and snapshot files (``JsonBackend``)
- ``error_code.rs``: error codes (``ErrorCode``)
- ``log.rs``: logging with context ``PERS``
- ``kvs_mock.rs``: mock of the API for tests of users (``MockKvs``)

C++ implementation (``score/kvs``):

- ``kvs.hpp`` / ``kvs.cpp``: API and implementation (``Kvs``, ``InstanceId``, ``SnapshotId``)
- ``kvsbuilder.hpp`` / ``kvsbuilder.cpp``: creation of instances (``KvsBuilder``)
- ``kvsvalue.hpp`` / ``kvsvalue.cpp``: value representation (``KvsValue``)
- ``error.hpp`` / ``error.cpp``: error codes and error domain (``ErrorCode``, ``KvsErrorDomain``)
- ``internal/kvs_helper.hpp`` / ``internal/kvs_helper.cpp``: hash calculation and conversion between ``KvsValue``
  and the baselibs JSON representation

The interface documentation of the units is part of the source code.
