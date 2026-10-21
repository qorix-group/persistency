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

KVS Component Architecture Documentation
========================================

.. document:: KVS Component Architecture
   :id: doc__kvs_component_architecture
   :status: valid
   :version: 1
   :safety: ASIL_B
   :security: YES
   :realizes: wp__component_arch[version==1]
   :tags: persistency

Overview
--------

The KVS component realizes the feature Persistency (:need:`feat__persistency`). It stores key-value pairs
of an application persistently in the file system, provides default values and keeps snapshots of the
persisted data. The component is implemented in C++ and in Rust with the same API concept.

.. comp:: persistency::kvs
   :id: comp__persistency_kvs
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :implements: logic_arc_int__persistency__interface[version==1]
   :uses: logic_arc_int__baselibs__json[version==1],logic_arc_int__baselibs__filesystem[version==1],logic_arc_int__log_cpp__logging[version==1]
   :belongs_to: feat__persistency[version==1]

Requirements Linked to Component Architecture
---------------------------------------------

The following component requirements are linked to the static view and the interfaces of the component:

.. needtable:: Overview of Component Requirements
   :style: table
   :columns: title;id
   :filter: type == "comp_req" and id.startswith("comp_req__kvs__") and len(fulfils_back) > 0
   :colwidths: 70,30

Description
-----------

An application opens a KVS instance with a builder (``KvsBuilder``), which defines the instance ID, the
snapshot to load and whether persisted data and default values are required. All key-value pairs of an
instance are held in memory. Persisted data is loaded when the instance is opened and written only when
the application calls ``flush``. Each data file is stored together with an Adler-32 hash file, which is
checked when the data is loaded. Before a flush the existing files are kept as snapshots.

Design decisions:

- The data is stored in JSON format in the file system (:need:`comp_req__kvs__pers_data_store_fmt`,
  :need:`comp_req__kvs__pers_data_store_bnd`).
- C++: the file system, JSON parser and writer and logger are used via the interfaces of the S-CORE baselibs
  (:need:`logic_arc_int__baselibs__filesystem`, :need:`logic_arc_int__baselibs__json`,
  :need:`logic_arc_int__log_cpp__logging`).
- Rust: the storage is accessed via the ``KvsBackend`` trait, the default backend ``JsonBackend`` uses the
  Rust standard library file API and the crates ``tinyjson`` and ``adler32``.

Design constraints:

- The API is synchronous. Asynchronous flush (:need:`comp_req__kvs__async_api`) and change callbacks
  (:need:`comp_req__kvs__callback_support`) are not realized.
- Snapshots are created only by ``flush``. Explicit snapshot creation, removal and slot query
  (:need:`comp_req__kvs__snapshot_create_api`, :need:`comp_req__kvs__snapshot_remove_api`,
  :need:`comp_req__kvs__snapshot_slot_free_api`) are not realized.
- The component does not prevent that an instance is opened by more than one process
  (:need:`comp_req__kvs__single_process_access` is not realized).

Rationale Behind Architecture Decomposition
*******************************************

The component is not split into internal components. It provides one functionality (persistent key-value
storage) with a small set of interfaces, and its parts are only used together. The C++ and the Rust
implementation are language bindings of the same component (:need:`comp_req__kvs__language_bindings`).
The decomposition into units is described in the detailed design (:need:`doc__kvs_detailed_design`).

Static Architecture
-------------------

.. comp_arc_sta:: KVS Static View
   :id: comp_arc_sta__persistency_kvs__sv
   :status: valid
   :version: 1
   :safety: ASIL_B
   :security: NO
   :uses: logic_arc_int__baselibs__json[version==1],logic_arc_int__baselibs__filesystem[version==1],logic_arc_int__log_cpp__logging[version==1]
   :fulfils: comp_req__kvs__pers_data_store_bnd[version==1],comp_req__kvs__pers_data_store_fmt[version==1],comp_req__kvs__value_serialize[version==1]
   :belongs_to: comp__persistency_kvs[version==1]

   .. needarch::
      :scale: 50
      :align: center

      {{ draw_component(need(), needs) }}

Dynamic Architecture
--------------------

KVS is the only component of the feature. The interactions of the component with its user and with the
used interfaces are described in the dynamic views of the feature architecture
(:need:`doc__persistency_kvs_architecture`), therefore no additional dynamic view is created on component level.

Interfaces
----------

The component provides the following public interfaces for C++ and Rust. They realize the logical interface
IKvs (:need:`logic_arc_int__persistency__interface`), which is implemented by :need:`comp__persistency_kvs`.
The realized IKvs operation is named in the description of each interface operation. The ``implements`` link
from the interface operations to the logical interface operations is added as soon as the operations of IKvs
are defined as ``logic_arc_int_op`` in the platform documentation.

The interface documentation (parameters, return values, error codes) is part of the source code.

.. real_arc_int:: score::mw::per::kvs::KvsBuilder
   :id: real_arc_int__kvs__cpp_builder
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :language: cpp
   :fulfils: comp_req__kvs__open_need_kvs_behavior[version==1], comp_req__kvs__default_value_cfg[version==2], comp_req__kvs__snapshot_max_num_cfg[version==1]

   Creates and opens a KVS instance. The options are set with chained calls, ``build()`` opens the instance. Source: ``score/kvs/kvsbuilder.hpp``.

.. real_arc_int_op:: KvsBuilder
   :id: real_arc_int_op__kvs__cpp_bld_new
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_builder[version==1]

   Creates a builder for the instance with the given ``InstanceId``.

.. real_arc_int_op:: snapshot
   :id: real_arc_int_op__kvs__cpp_bld_snapshot
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_builder[version==1]

   Selects the snapshot that is loaded when the instance is opened.

.. real_arc_int_op:: need_defaults_flag
   :id: real_arc_int_op__kvs__cpp_bld_need_defaults
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_builder[version==1]

   Defines whether default values are required (``OpenNeedDefaults``).

.. real_arc_int_op:: need_kvs_flag
   :id: real_arc_int_op__kvs__cpp_bld_need_kvs
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_builder[version==1]

   Defines whether persisted data is required (``OpenNeedKvs``).

.. real_arc_int_op:: dir
   :id: real_arc_int_op__kvs__cpp_bld_dir
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_builder[version==1]

   Sets the directory of the storage files.

.. real_arc_int_op:: max_storage_bytes
   :id: real_arc_int_op__kvs__cpp_bld_max_storage
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_builder[version==1]

   Sets the maximum total storage size in bytes, no limit if not set.

.. real_arc_int_op:: snapshot_max_count
   :id: real_arc_int_op__kvs__cpp_bld_snapshot_max
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_builder[version==1]

   Sets the maximum number of snapshots, values above ``KVS_MAX_SNAPSHOTS_UPPER_BOUND`` (3) are clamped.

.. real_arc_int_op:: build
   :id: real_arc_int_op__kvs__cpp_bld_build
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_builder[version==1]

   Opens the instance with the configured options. Realizes the IKvs operation ``open`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int:: score::mw::per::kvs::Kvs
   :id: real_arc_int__kvs__cpp_kvs
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :language: cpp
   :fulfils: comp_req__kvs__language_bindings[version==1], comp_req__kvs__key_length[version==1], comp_req__kvs__key_exists_api[version==1], comp_req__kvs__key_list_api[version==1], comp_req__kvs__key_remove_api[version==1], comp_req__kvs__value_data_types[version==2], comp_req__kvs__value_get_api[version==1], comp_req__kvs__value_set_api[version==1], comp_req__kvs__value_default[version==2], comp_req__kvs__value_reset[version==2], comp_req__kvs__default_value_query[version==2], comp_req__kvs__default_value_is_default_api[version==1], comp_req__kvs__store_reset_api[version==1], comp_req__kvs__discard_pending_changes_api[version==1], comp_req__kvs__explicit_flush[version==1], comp_req__kvs__recovery_snapshot_restore[version==1], comp_req__kvs__snapshot_restore_api[version==1], comp_req__kvs__concurrency[version==1]

   Access to the key-value pairs of an opened KVS instance. Values are represented by ``KvsValue`` (``score/kvs/kvsvalue.hpp``), errors by ``ErrorCode`` (``score/kvs/error.hpp``). Source: ``score/kvs/kvs.hpp``.

.. real_arc_int_op:: reset
   :id: real_arc_int_op__kvs__cpp_reset
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Removes all key-value pairs from the instance. Realizes the IKvs operation ``reset`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: get_all_keys
   :id: real_arc_int_op__kvs__cpp_get_all_keys
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Returns all keys that have a written value. Realizes the IKvs operation ``get_all_keys`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: key_exists
   :id: real_arc_int_op__kvs__cpp_key_exists
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Checks if a value was written for the key. Realizes the IKvs operation ``key_exists`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: get_value
   :id: real_arc_int_op__kvs__cpp_get_value
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Returns the written value of the key or its default value. Realizes the IKvs operation ``get_value`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: get_default_value
   :id: real_arc_int_op__kvs__cpp_get_default_value
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Returns the default value of the key. Realizes the IKvs operation ``get_default_value`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: reset_key
   :id: real_arc_int_op__kvs__cpp_reset_key
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Removes the written value of the key, so that the default value applies.

.. real_arc_int_op:: is_value_default
   :id: real_arc_int_op__kvs__cpp_is_value_default
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Checks if the key has no written value and a default value exists.

.. real_arc_int_op:: set_value
   :id: real_arc_int_op__kvs__cpp_set_value
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Sets the value of the key. Realizes the IKvs operation ``set_value`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: remove_key
   :id: real_arc_int_op__kvs__cpp_remove_key
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Removes the key and its written value. Realizes the IKvs operation ``remove_key`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: remove_all_keys
   :id: real_arc_int_op__kvs__cpp_remove_all_keys
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Removes all key-value pairs from the instance.

.. real_arc_int_op:: discard_pending_changes
   :id: real_arc_int_op__kvs__cpp_discard_changes
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Reloads the persisted data and discards all changes since the last flush.

.. real_arc_int_op:: flush
   :id: real_arc_int_op__kvs__cpp_flush
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Writes the data to the storage file and rotates the snapshots. Realizes the IKvs operation ``flush`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: snapshot_count
   :id: real_arc_int_op__kvs__cpp_snapshot_count
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Returns the number of existing snapshots. Realizes the IKvs operation ``snapshot_count`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: snapshot_max_count
   :id: real_arc_int_op__kvs__cpp_snapshot_max_count
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Returns the configured maximum number of snapshots. Realizes the IKvs operation ``snapshot_max_count`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: snapshot_restore
   :id: real_arc_int_op__kvs__cpp_snapshot_restore
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Restores the data of the instance from the given snapshot. Realizes the IKvs operation ``snapshot_restore`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: get_kvs_filename
   :id: real_arc_int_op__kvs__cpp_get_kvs_filename
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Returns the path of the data file of the given snapshot.

.. real_arc_int_op:: get_hash_filename
   :id: real_arc_int_op__kvs__cpp_get_hash_filename
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Returns the path of the hash file of the given snapshot.

.. real_arc_int_op:: get_storage_file_size
   :id: real_arc_int_op__kvs__cpp_get_storage_size
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Returns the size of the persisted data and hash file.

.. real_arc_int_op:: calculate_potential_size
   :id: real_arc_int_op__kvs__cpp_calc_potential_size
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__cpp_kvs[version==1]

   Returns the storage size after a flush and checks it against the storage limit.

.. real_arc_int:: rust_kvs::kvs_builder::KvsBuilder
   :id: real_arc_int__kvs__rust_builder
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :language: rust
   :fulfils: comp_req__kvs__multi_instance[version==2], comp_req__kvs__open_need_kvs_behavior[version==1], comp_req__kvs__default_value_cfg[version==2], comp_req__kvs__storage_backend_select[version==1]

   Creates and opens a KVS instance. The options are set with chained calls, ``build()`` opens the instance or returns the already opened instance with the same ``InstanceId`` if the options match. Source: ``score/kvs/rust_kvs/kvs_builder.rs``.

.. real_arc_int_op:: new
   :id: real_arc_int_op__kvs__rs_bld_new
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_builder[version==1]

   Creates a builder for the instance with the given ``InstanceId``.

.. real_arc_int_op:: max_instances
   :id: real_arc_int_op__kvs__rs_bld_max_instances
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_builder[version==1]

   Returns the maximum number of instances per process (``KVS_MAX_INSTANCES``).

.. real_arc_int_op:: snapshot
   :id: real_arc_int_op__kvs__rs_bld_snapshot
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_builder[version==1]

   Selects the snapshot that is loaded when the instance is opened.

.. real_arc_int_op:: defaults
   :id: real_arc_int_op__kvs__rs_bld_defaults
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_builder[version==1]

   Defines whether default values are required, optional or ignored (``KvsDefaults``).

.. real_arc_int_op:: kvs_load
   :id: real_arc_int_op__kvs__rs_bld_kvs_load
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_builder[version==1]

   Defines whether persisted data is required, optional or ignored (``KvsLoad``).

.. real_arc_int_op:: backend
   :id: real_arc_int_op__kvs__rs_bld_backend
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_builder[version==1]

   Sets the storage backend, ``JsonBackend`` if not set.

.. real_arc_int_op:: build
   :id: real_arc_int_op__kvs__rs_bld_build
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_builder[version==1]

   Opens the instance with the configured options. Realizes the IKvs operation ``open`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int:: rust_kvs::kvs_api::KvsApi
   :id: real_arc_int__kvs__rust_kvs_api
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :language: rust
   :fulfils: comp_req__kvs__language_bindings[version==1], comp_req__kvs__key_exists_api[version==1], comp_req__kvs__key_list_api[version==1], comp_req__kvs__key_remove_api[version==1], comp_req__kvs__value_data_types[version==2], comp_req__kvs__value_get_api[version==1], comp_req__kvs__value_set_api[version==1], comp_req__kvs__value_default[version==2], comp_req__kvs__value_reset[version==2], comp_req__kvs__default_value_query[version==2], comp_req__kvs__default_value_is_default_api[version==1], comp_req__kvs__store_reset_api[version==1], comp_req__kvs__explicit_flush[version==1], comp_req__kvs__recovery_snapshot_restore[version==1], comp_req__kvs__snapshot_restore_api[version==1], comp_req__kvs__concurrency[version==1]

   Access to the key-value pairs of an opened KVS instance, implemented by ``Kvs`` (``kvs.rs``) and by ``MockKvs`` (``kvs_mock.rs``) for tests of users. Values are represented by ``KvsValue`` (``kvs_value.rs``), errors by ``ErrorCode`` (``error_code.rs``). Source: ``score/kvs/rust_kvs/kvs_api.rs``.

.. real_arc_int_op:: reset
   :id: real_arc_int_op__kvs__rs_reset
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Removes all key-value pairs from the instance. Realizes the IKvs operation ``reset`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: reset_key
   :id: real_arc_int_op__kvs__rs_reset_key
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Removes the written value of the key, so that the default value applies.

.. real_arc_int_op:: get_all_keys
   :id: real_arc_int_op__kvs__rs_get_all_keys
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Returns all keys that have a written value. Realizes the IKvs operation ``get_all_keys`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: key_exists
   :id: real_arc_int_op__kvs__rs_key_exists
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Checks if a value was written for the key. Realizes the IKvs operation ``key_exists`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: get_value
   :id: real_arc_int_op__kvs__rs_get_value
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Returns the written value of the key or its default value. Realizes the IKvs operation ``get_value`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: get_value_as
   :id: real_arc_int_op__kvs__rs_get_value_as
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Returns the value of the key converted to the requested type.

.. real_arc_int_op:: get_default_value
   :id: real_arc_int_op__kvs__rs_get_default_value
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Returns the default value of the key. Realizes the IKvs operation ``get_default_value`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: is_value_default
   :id: real_arc_int_op__kvs__rs_is_value_default
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Checks if the key has no written value and a default value exists.

.. real_arc_int_op:: set_value
   :id: real_arc_int_op__kvs__rs_set_value
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Sets the value of the key. Realizes the IKvs operation ``set_value`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: remove_key
   :id: real_arc_int_op__kvs__rs_remove_key
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Removes the key and its written value. Realizes the IKvs operation ``remove_key`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: flush
   :id: real_arc_int_op__kvs__rs_flush
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Writes the data to the storage backend. Realizes the IKvs operation ``flush`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: snapshot_count
   :id: real_arc_int_op__kvs__rs_snapshot_count
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Returns the number of existing snapshots. Realizes the IKvs operation ``snapshot_count`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: snapshot_max_count
   :id: real_arc_int_op__kvs__rs_snapshot_max_count
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Returns the maximum number of snapshots. Realizes the IKvs operation ``snapshot_max_count`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int_op:: snapshot_restore
   :id: real_arc_int_op__kvs__rs_snapshot_restore
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_kvs_api[version==1]

   Restores the data of the instance from the given snapshot. Realizes the IKvs operation ``snapshot_restore`` of :need:`logic_arc_int__persistency__interface`.

.. real_arc_int:: rust_kvs::kvs_backend::KvsBackend
   :id: real_arc_int__kvs__rust_backend
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :language: rust
   :fulfils: comp_req__kvs__storage_backend_select[version==1]

   Interface of the storage backend. It is implemented by ``JsonBackend`` (``json_backend.rs``) and can be implemented by users for other storage backends. Source: ``score/kvs/rust_kvs/kvs_backend.rs``.

.. real_arc_int_op:: load_kvs
   :id: real_arc_int_op__kvs__rs_be_load_kvs
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_backend[version==1]

   Loads the data of the given instance and snapshot.

.. real_arc_int_op:: load_defaults
   :id: real_arc_int_op__kvs__rs_be_load_defaults
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_backend[version==1]

   Loads the default values of the given instance.

.. real_arc_int_op:: flush
   :id: real_arc_int_op__kvs__rs_be_flush
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_backend[version==1]

   Writes the data of the given instance and rotates the snapshots.

.. real_arc_int_op:: snapshot_count
   :id: real_arc_int_op__kvs__rs_be_snapshot_count
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_backend[version==1]

   Returns the number of existing snapshots of the given instance.

.. real_arc_int_op:: snapshot_max_count
   :id: real_arc_int_op__kvs__rs_be_snapshot_max
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_backend[version==1]

   Returns the maximum number of snapshots.

.. real_arc_int_op:: snapshot_restore
   :id: real_arc_int_op__kvs__rs_be_snapshot_restore
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_backend[version==1]

   Loads the data of the given snapshot.

.. real_arc_int:: rust_kvs::json_backend::JsonBackendBuilder
   :id: real_arc_int__kvs__rust_json_builder
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :language: rust
   :fulfils: comp_req__kvs__snapshot_max_num_cfg[version==1], comp_req__kvs__pers_data_store_fmt[version==1]

   Creates the JSON storage backend ``JsonBackend``. Source: ``score/kvs/rust_kvs/json_backend.rs``.

.. real_arc_int_op:: new
   :id: real_arc_int_op__kvs__rs_jbb_new
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_json_builder[version==1]

   Creates a builder with the current directory and 3 snapshots.

.. real_arc_int_op:: working_dir
   :id: real_arc_int_op__kvs__rs_jbb_working_dir
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_json_builder[version==1]

   Sets the directory of the storage files.

.. real_arc_int_op:: snapshot_max_count
   :id: real_arc_int_op__kvs__rs_jbb_snapshot_max
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_json_builder[version==1]

   Sets the maximum number of snapshots.

.. real_arc_int_op:: build
   :id: real_arc_int_op__kvs__rs_jbb_build
   :security: YES
   :safety: ASIL_B
   :status: valid
   :version: 1
   :included_by: real_arc_int__kvs__rust_json_builder[version==1]

   Creates the ``JsonBackend``.

Internal Components
-------------------

The component has no internal components, see `Rationale Behind Architecture Decomposition`_.
