import { GluegunToolbox } from 'gluegun'

module.exports = {
  name: 'kafka-cli',
  run: async (toolbox: GluegunToolbox) => {
    const { print } = toolbox

    print.info('Welcome to your CLI')
  }
}
