# -*- encoding : utf-8 -*-
module Cequel
  module Record
    #
    # `Cequel::Record` implementations define their own schema in their class
    # definitions. As well as defining attributes on record instances, the
    # column definitions in {Properties} allow a `Cequel::Record` to have a
    # precise internal represntation of its representation as a CQL3 table
    # schema. Further, it is able to check this representation against the
    # actual table defined in Cassandra (if any), and create or modify the
    # schema in Cassandra to match what's defined in code.
    #
    # All the interesting stuff is in the {ClassMethods}.
    #
    # @since 1.0.0
    #
    module Schema
      extend ActiveSupport::Concern
      extend Forwardable

      included do
        class_attribute :table_name, instance_writer: false
        class_attribute :counter_table_name, instance_writer: false
        unless name.nil?
          self.table_name = name.tableize.to_sym unless name.nil?
          self.counter_table_name = :"#{table_name.singularize}_counts"
        end
      end

      #
      # Methods available on {Record} class singletons to introspect and modify
      # the schema defined in the database
      #
      module ClassMethods
        #
        # @!attr table_name
        #   @return [Symbol] name of the CQL table that backs this record class
        #

        extend Forwardable

        #
        # @!attribute [r] columns
        #   (see Cequel::Schema::Table#columns)
        #
        # @!attribute [r] key_columns
        #   (see Cequel::Schema::Table#key_columns)
        #
        # @!attribute [r] key_column_names
        #   (see Cequel::Schema::Table#key_column_names)
        #
        # @!attribute [r] partition_key_columns
        #   (see Cequel::Schema::Table#partition_key_columns)
        #
        # @!attribute [r] partition_key_column_names
        #   (see Cequel::Schema::Table#partition_key_column_names)
        #
        # @!attribute [r] clustering_columns
        #   (see Cequel::Schema::Table#clustering_columns)
        #
        # @!method compact_storage?
        #   (see Cequel::Schema::Table#compact_storage?)
        #
        def_delegators :table_schema, :columns, :key_columns,
                       :key_column_names, :partition_key_columns,
                       :partition_key_column_names, :clustering_columns,
                       :compact_storage?

        # (see Cequel::Schema::Table#column)
        def reflect_on_column(name)
          table_schema.column(name) || counter_table_schema.column(name)
        end

        #
        # Read the current schema assigned to this record's table from
        # Cassandra, and make any necessary modifications (including creating
        # the table for the first time) so that it matches the schema defined
        # in the record definition
        #
        # @raise (see Schema::TableSynchronizer.apply)
        # @return [void]
        #
        def synchronize_schema
          if has_scalar_table?
            Cequel::Schema::TableSynchronizer
              .apply(connection, read_schema, table_schema)
          end
          if has_counter_table?
            Cequel::Schema::TableSynchronizer
              .apply(connection, read_counter_schema, counter_table_schema)
          end
        end

        #
        # Read the current state of this record's table in Cassandra from the
        # database.
        #
        # @return [Schema::Table] the current schema assigned to this record's
        #   table in the database
        #
        def read_schema
          connection.schema.read_table(table_name)
        end

        #
        # Read the current state of this record's companion counter table in
        # Cassandra from the database.
        #
        # @return [Schema::Table] the current schema assigned to this record's
        #   counter table in the database
        #
        def read_counter_schema
          connection.schema.read_table(counter_table_name)
        end

        #
        # @return [Schema::Table] the schema as defined by the columns
        #   specified in the class definition
        #
        def table_schema
          @table_schema ||= initialize_schema(table_name)
        end

        def counter_table_schema
          @counter_table_schema ||= initialize_schema(counter_table_name)
        end

        def has_scalar_table?
          table_schema.data_columns.any? || !has_counter_table?
        end

        def has_counter_table?
          counter_table_schema.data_columns.any?
        end

        def each_table_schema
          return enum_for(:each_table_schema) unless block_given?

          yield table_schema
          yield counter_table_schema
        end

        def defined_table_schemas
          [].tap do |schemas|
            schemas << table_schema if has_scalar_table?
            schemas << counter_table_schema if has_counter_table?
          end
        end

        protected

        def key(name, type, options = {})
          super
          each_table_schema do |schema|
            if options[:partition]
              schema.add_partition_key(name, type)
            else
              schema.add_key(name, type, options[:order])
            end
          end
        end

        def column(name, type, options = {})
          super
          table_schema.add_data_column(name, type, options[:index])
        end

        def list(name, type, options = {})
          super
          table_schema.add_list(name, type)
        end

        def set(name, type, options = {})
          super
          table_schema.add_set(name, type)
        end

        def map(name, key_type, value_type, options = {})
          super
          table_schema.add_map(name, key_type, value_type)
        end

        def counter(name)
          super
          counter_table_schema.add_data_column(name, :counter)
        end

        def table_property(name, value)
          each_table_schema { |schema| schema.add_property(name, value) }
        end

        def compact_storage
          each_table_schema { |schema| schema.compact_storage = true }
        end

        private

        def initialize_schema(name)
          Cequel::Schema::Table.new(name)
        end
      end

      protected

      def_delegator 'self.class', :table_schema
      protected :table_schema
    end
  end
end
