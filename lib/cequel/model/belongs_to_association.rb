module Cequel

  module Model

    class BelongsToAssociation

      extend Forwardable

      attr_reader :clazz, :name, :parent_class_name
      def_delegator :parent_key, :type, :key_type

      def initialize(clazz, name, parent_class_name)
        @class, @name, @parent_class_name =
          clazz, name.to_sym, parent_class_name.to_sym
      end

      def parent_key
        @parent_key ||= parent_class.local_key_column
      end

      def parent_class
        @parent_class ||= parent_class_name.to_s.constantize
      end

      def key_name
        partition_key_name = :"#{name}_#{parent_key.name}"
      end

    end

  end

end
