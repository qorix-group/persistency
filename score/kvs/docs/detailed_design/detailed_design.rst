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
   :security: YES
   :realizes: wp__sw_implementation[version==1]
   :tags: persistency

Description
-----------

The KVS component (:need:`comp__persistency_kvs`) is implemented in Rust and in C++ with the same API concept.
The architecture of the component is described in :need:`doc__kvs_component_architecture`. The public
interfaces of the component and their operations are defined there:

- C++: :need:`real_arc_int__kvs__cpp_builder` and :need:`real_arc_int__kvs__cpp_kvs`
- Rust: :need:`real_arc_int__kvs__rust_builder`, :need:`real_arc_int__kvs__rust_kvs_api`,
  :need:`real_arc_int__kvs__rust_backend` and :need:`real_arc_int__kvs__rust_json_builder`

This document describes the decomposition of the component into units and their interactions.

Design decisions:

- An instance is created with a builder (``KvsBuilder``), which defines the instance ID, the snapshot to load and
  whether persisted data and default values are required, optional or ignored.
- All values of an instance are held in memory. Persisted data is loaded on open and written on explicit flush.
- Each instance is protected by one mutex for thread-safe access (:need:`comp_req__kvs__concurrency`).
- The data is stored as JSON data file with an Adler-32 hash file, snapshots are stored as additional file pairs.
  On flush the data and hash file are synchronized to the storage (C++: ``fdatasync``, Rust: ``sync_data``).
- Rust: the storage access is encapsulated in the ``KvsBackend`` trait. The default backend ``JsonBackend``
  uses the crates ``tinyjson`` and ``adler32`` and the Rust standard library file API. Logging uses ``score_log``
  with the context ``PERS``.
- C++: the file system, JSON parser and writer and logger of the S-CORE baselibs are injected into ``Kvs``
  as interfaces, which allows testing with mocks.

Design constraints:

- Rust: at most ``KVS_MAX_INSTANCES`` (10) instances exist per process. ``KvsBuilder`` returns the existing
  instance when an instance ID is opened again with the same options, otherwise ``InstanceParametersMismatch``.
- Rust: the maximum number of snapshots is set in ``JsonBackendBuilder`` (default 3).
- C++: the API functions do not wait for the mutex and return ``MutexLockFailed`` if the instance is locked.
- C++: keys must not be empty and are limited to ``KVS_MAX_KEY_LENGTH_BYTES`` (32 bytes).
- C++: the maximum number of snapshots is limited to ``KVS_MAX_SNAPSHOTS_UPPER_BOUND`` (3), default
  ``KVS_DEFAULT_MAX_SNAPSHOTS`` (3). An optional storage limit (``max_storage_bytes``) is checked on flush before the files are written.

Rationale Behind Decomposition into Units
*****************************************

The units follow the single responsibility principle: API definition, implementation of the API, instance
creation, value representation, storage backend, serialization and error handling are separate units. The
separation of the storage backend (Rust) and the injection of the baselibs interfaces (C++) allow testing the
KVS logic independent of the file system.

Static Diagrams for Unit Interactions
-------------------------------------

The static diagrams show the units of the component, their relations and the used libraries. The operations
of the units are documented in the source code and, for the public interfaces, in the component architecture.

``dd_sta__kvs__rust_units``: units of the Rust implementation. ``Kvs`` implements ``KvsApi`` and accesses the
storage via ``KvsBackend``, ``KvsBuilder`` creates ``Kvs`` with ``JsonBackend`` as default backend.

.. uml:: dd_sta__kvs__rust_units.puml

``dd_sta__kvs__cpp_units``: units of the C++ implementation. ``KvsBuilder`` creates ``Kvs``, which uses the
injected baselibs interfaces and ``kvs_helper`` for the hash calculation and the conversion of ``KvsValue``.

.. uml:: dd_sta__kvs__cpp_units.puml

Dynamic Diagrams for Unit Interactions
--------------------------------------

The interactions of the component with its user and the used interfaces are described in the dynamic views of
the feature architecture (:need:`doc__persistency_kvs_architecture`). Within the component the interactions
between the units are direct function calls in the same order, so no additional dynamic diagram is needed.

Units within the Component
--------------------------

Rust implementation (``score/kvs/rust_kvs``):

- ``kvs_api.rs``: API definition (``KvsApi`` trait, ``InstanceId``, ``SnapshotId``, ``KvsDefaults``, ``KvsLoad``)
- ``kvs.rs``: implementation of the API (``Kvs``, ``KvsParameters``)
- ``kvs_builder.rs``: creation of instances and instance pool (``KvsBuilder``)
- ``kvs_value.rs``: value representation (``KvsValue``, ``KvsMap``)
- ``kvs_serialize.rs``: conversion of custom types to and from ``KvsValue`` (``KvsSerialize``, ``KvsDeserialize``)
- ``kvs_backend.rs``: storage backend interface (``KvsBackend``)
- ``json_backend.rs``: JSON storage backend with hash and snapshot files (``JsonBackend``, ``JsonBackendBuilder``)
- ``error_code.rs``: error codes (``ErrorCode``)
- ``log.rs``: logging macros with context ``PERS``
- ``kvs_mock.rs``: mock of the API for tests of users (``MockKvs``)

C++ implementation (``score/kvs``):

- ``kvs.hpp`` / ``kvs.cpp``: API and implementation (``Kvs``, ``InstanceId``, ``SnapshotId``)
- ``kvsbuilder.hpp`` / ``kvsbuilder.cpp``: creation of instances (``KvsBuilder``)
- ``kvsvalue.hpp`` / ``kvsvalue.cpp``: value representation (``KvsValue``)
- ``error.hpp`` / ``error.cpp``: error codes and error domain (``ErrorCode``, ``KvsErrorDomain``)
- ``internal/kvs_helper.hpp`` / ``internal/kvs_helper.cpp``: hash calculation and conversion between ``KvsValue``
  and the baselibs JSON representation

The interface documentation of the units is part of the source code.
