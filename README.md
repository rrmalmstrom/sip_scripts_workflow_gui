# SIP Workflow Scripts for GUI Application

This repository contains the Python workflow scripts used by the SIP LIMS Workflow Manager GUI application.

## About

These scripts implement the laboratory workflow steps for Stable Isotope Probing (SIP) analysis, including:

- Sample setup and plate preparation
- Ultracentrifuge processing
- Density analysis and plotting
- Library creation and QC
- Pooling and finalization steps

## Usage

These scripts are designed to be executed by the SIP LIMS Workflow Manager GUI application. They should not be run directly from the command line as they expect specific working directory context and file structures provided by the GUI.

## Integration with GUI Application

- **Repository**: This is the updated scripts repository for the SIP LIMS Workflow Manager
- **Setup**: Scripts are automatically cloned by the GUI application's setup process
- **Updates**: Use the GUI application's update mechanism to get the latest script versions
- **Working Directory**: Scripts run with the project folder as the current working directory

## Script Requirements

All scripts follow these conventions:

1. **Success Markers**: Create `.workflow_status/{script_name}.success` files upon successful completion
2. **Relative Paths**: Use relative paths assuming project folder as working directory
3. **Error Handling**: Proper exit codes and error reporting for GUI integration

## Version

This repository contains the updated scripts developed alongside the GUI application. For legacy scripts, see the original `sip_scripts` repository.

## Development

When modifying scripts:

1. Test with the GUI application
2. Ensure success marker creation on completion
3. Verify proper error handling and rollback behavior
4. Update this README if new scripts are added

## Support

For issues with scripts, please refer to the main SIP LIMS Workflow Manager documentation or contact the development team.