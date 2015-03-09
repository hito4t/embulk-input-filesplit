Embulk::JavaPlugin.register_input(
  :filesplit, "org.embulk.input.filesplit.LocalFileSplitInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
