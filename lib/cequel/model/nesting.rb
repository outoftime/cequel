module Cequel

  module Model

    module Nesting

      extend ActiveSupport::Concern

      included do
        class_attribute :parent_association
      end

      module ClassMethods

        def belongs_to(parent, options = {})
          parent_class_name = options.fetch(:class_name) { parent.to_s.classify }
          self.parent_association =
            BelongsToAssociation.new(self, parent, parent_class_name)

          table_schema.add_partition_key(
            parent_association.key_name, parent_association.key_type)
          def_accessors(parent_association.key_name)
          def_belongs_to_accessors(parent_association.name)
          set_attribute_default(parent_association.key_name, nil)
        end

        def has_many(*args)
        end

        private

        def def_belongs_to_accessors(name)
          module_eval <<-RUBY, __FILE__, __LINE__+1
            def #{name}
              return @#{name} if defined? @#{name}
              key = parent_association.key_name
              key_attribute = read_attribute(key)
              @#{name} = key_attribute ?
                parent_association.parent_class[key_attribute] :
                nil
            end
          RUBY
        end

      end

    end

  end

end
