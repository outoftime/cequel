module Cequel

  module Model

    module Schema

      extend ActiveSupport::Concern

      included do
        class_attribute :table_schema_builders
        self.table_schema_builders = []
      end

      module ClassMethods

        def synchronize_schema
          Cequel::Schema::TableSynchronizer.
            apply(connection, read_schema, table_schema)
        end

        def read_schema
          connection.schema.read_table(table_name)
        end

        def table_schema
          @table_schema ||= Cequel::Schema::Table.new(table_name)
        end

        def local_key_column
          warn "Called local_key_column from #{caller.first}"
          @local_key_column ||= table_schema.key_columns.last
        end

      end

    end

  end

end
