<%@ taglib uri="http://java.sun.com/jstl/core_rt" prefix="c"%>

<link rel="stylesheet" type="text/css" media="all" href="<%=request.getContextPath()%>/css/daterangepicker-bs3.css"/>
<script type="text/javascript" src="<%=request.getContextPath()%>/js/plugin/datepicker/moment.js"></script>
<script type="text/javascript" src="<%=request.getContextPath()%>/js/plugin/datepicker/daterangepicker.js"></script>
<script type="text/javascript" src="<%=request.getContextPath()%>/js/plugin/loading-indicator/jquery.loading-indicator.js"></script>
<link type="text/css" href="<%=request.getContextPath()%>/css/jquery.loading-indicator.css" rel="stylesheet"/>
<script type="text/javascript" src="<%=request.getContextPath()%>/js/plugin/datatables/custom-dateOptions.js"></script>

<!-- widget grid -->
<section id="widget-grid" class="">

	<!-- row -->
	<div class="row">
		
		<!-- NEW WIDGET START -->
		<article class="col-xs-12 col-sm-12 col-md-12 col-lg-12">
			
			<!-- Widget ID (each widget will need unique ID)-->
			<div class="jarviswidget jarviswidget-color-blueDark" id="wid-id-0"
				 data-widget-colorbutton="false"
				 data-widget-editbutton="false"
				 data-widget-togglebutton="false"
				 data-widget-deletebutton="false"
				 data-widget-fullscreenbutton="false"
				 data-widget-custombutton="false"
				 data-widget-collapsed="false"
				 data-widget-sortable="false">
				<header>
					<span class="widget-icon"> <i class="fa fa-table"></i> </span>
					<h2>Biometric Events</h2>
					<h2 class="pull-right" style="padding-right:1%">
						<label>Auto Refresh
							<span class="onoffswitch">
								<input type="checkbox" class="onoffswitch-checkbox" id="autoRefreshBiometricEvents" onChange="triggerAutoRefresh(this);">
								<label class="onoffswitch-label" for="autoRefreshBiometricEvents">
									<span class="onoffswitch-inner" data-swchon-text="YES" data-swchoff-text="NO"></span>
									<span class="onoffswitch-switch"></span>
								</label>
							</span>
                        </label>
					</h2>
				</header>

				<!-- widget div-->
				<div>
					
					<!-- widget edit box -->
					<div class="jarviswidget-editbox">
						<!-- This area used as dropdown edit box -->
						<input class="form-control" type="text">	
					</div>
					<!-- end widget edit box -->
					
					<!-- widget content -->
					<div class="widget-body no-padding">
						<table id="biometricevent-info-table" class="table table-striped table-hover table-bordered noWrapCss" width="100%">
							<thead>
								<tr class="no-print" id="tblSearch">
									<th id="search_biometricId_th">Biometric Id</th>
									<th id="search_externalId_th">External Id</th>
									<th id="search_eventId_th">Event Id</th>
									<th id="search_binId_th">Bin Id</th>
									<th id="search_assignedSegmentId_th">Segment Id</th>
									<th id="search_status_th">Status</th>
									<th id="search_phase_th">Phase</th>
									<th id="search_dataVersion_th">Data Version</th>
									<th id="search_updateDateTime_th">Update Time</th>
								</tr>
								<tr id="tblHeader">
									<th id="biometricId_th">Biometric Id</th>
									<th id="externalId_th">External Id</th>
									<th id="eventId_th">Event Id</th>
									<th id="binId_th">Bin Id</th>
									<th id="assignedSegmentId_th">Segment Id</th>
									<th id="status_th">Status</th>
									<th id="phase_th">Phase</th>
									<th id="dataVersion_th">Data Version</th>
									<th id="updateDateTime_th">Update Time</th>
								</tr>						
							</thead>
							<tbody>
	                        </tbody>
						</table>
					</div>
				</div>
			</div>
		</article>
	</div>				
	<!-- end widget content -->
	<!-- end row -->

<jsp:include page="templateDetailsPopup.jsp"></jsp:include>

</section>
<!-- end widget grid -->

<script type="text/javascript">

	$(document).ready(function() {
		//initializeLoader();
		//drawBiometricEventInfoTable();
	});
	
	function initializeLoader(){
        loader= $('body').loadingIndicator({
            useImage: false
        }).data("loadingIndicator");
        loader.hide();
    }
	
	var otable;
	//var dateFormat = 'MM/DD/YYYY';
	var cb2;
	var cb1;
	var intervalID = 0;
	
	var drawBiometricEventInfoTable = function() {
		//loader.show();
		/* COLUMN FILTER  */
		var biometricEventsListUrl = "<%=request.getContextPath()%>/secured/admin/bioevent/getBiometricEvents.htm";
		
		otable = $('#biometricevent-info-table').DataTable({
			"processing": false,
			"serverSide": true,
			"scrollY": 400,
			"scrollX":true,
            "scrollCollapse": true,
            "pageLength": 10,
			"pagingType": "full_numbers",
			"order": [[ 0, 0 ]],
			"destroy":true,
			"sDom": "<'dt-toolbar'<'col-xs-12 col-sm-6'><'col-sm-6 col-xs-12 hidden-xs'lT>>"+
						"rt"+
						"<'dt-toolbar-footer'<'col-sm-6 col-xs-12 hidden-xs'i><'col-xs-12 col-sm-6'p>>",						
			"autoWidth" : true,
			"ajax": {
				"url": biometricEventsListUrl,
				"type": "GET",
				"cache": false,
				 error : function(XMLHttpRequest, textStatus, errorThrown)  {
						$.smallBox({
							title : "Error",
							content : ""+XMLHttpRequest.responseText+"",
							color : "#C44221",
							icon : "fa fa-info",
							timeout : 5000
						});
				}
			},
			"columns": [ 
				{"data": "biometricId", "render": function ( data, type, row ) {
						return "<a href='javascript:getTemplateDetails(\""+ row.biometricId+"\")'>"+row.biometricId+"</a>";
					}
				},
				{"data": "externalId", "defaultContent": ""},
				{"data": "eventId", "defaultContent": "", "sWidth":"100px"},
				{"data": "binId", "defaultContent": "", "sWidth":"100px"},
				{"data": "assignedSegmentId", "defaultContent": "", "sWidth":"100px"},
				{"data": "status", "defaultContent": "", "sWidth":"100px"},
				{"data": "phase", "defaultContent": "", "sWidth":"100px"},
				{"data": "dataVersion", "defaultContent": "", "sWidth":"100px"},
				{"data": "updateDateTime", "defaultContent": "", "sWidth":"220px"}
			],
			"oTableTools": {
				"sRowSelect": "single",
				"aButtons": [
					{
						"sExtends":    "text",
						"sButtonClass": "btn btn-labeled btn-danger",
						"sButtonText": "<span class='btn-label'><i class='fa fa-times'></i></span>Reset Filters",
						"fnClick": function ( nButton, oConfig, oFlash ) {
							resetAllFields();//This will reset the filters applied.
						}
					}
				]
			},
            "aoColumnDefs": [
			{
				"aTargets": [],
				"orderable": false
			}
			],
			"fnRowCallback": function (row, data) {
				$(row).attr("id",data.functionId);
			},
            "fnDrawCallback": function() {
                //loader.hide();//The loader should be hidden only after the ajax completes and should be called irrespective of the return data.
            }
		});
		//$("#ToolTables_biometricevent-info-table_0").css("display","none");
		console.log(otable.columns());
		
		otable.columns().eq( 0 ).each( function ( colIdx ) {
			var id = '#search_' + otable.column( colIdx ).header().id;
			console.log(id);
			console.log(this.value);
			$( 'input', $(id)[0]).on( 'keyup change', function (e) {
				var keyCode = e.keyCode || e.which;
				if (keyCode === 13) {
					//loader.show();
					otable.column( colIdx ).search( this.value ).draw();
				}
				if (keyCode === 8 && this.value == '') {
					//loader.show();
					otable.column( colIdx ).search( this.value ).draw();
				}
			});
			
			$( 'select',  $(id)[0]).on( 'change', function (e) {
					otable.column( colIdx ).search( this.value ).draw();
			});
			
			$('#date_search_updateDateTime_th',$(id)[0]).on('apply.daterangepicker', function(ev, picker) { 
				var startDateTime = picker.startDate.format(dateFormat);
				//var startDate = startDateTime.substring(0,10);
				var endDateTime = picker.endDate.format(dateFormat);
				//var endDate = endDateTime.substring(0,10);
				var dateRange=startDateTime+' - '+endDateTime; 
				$('#date_search_updateDateTime_th span').html(dateRange + ' <i class="fa fa-calendar"></i>');
				otable.column( colIdx ).search( dateRange).draw();
			});
			
		});
		
	};
	
		//Add column filters	
		$('#biometricevent-info-table thead #tblSearch th').each( function () {
			var title = $('#biometricevent-info-table thead #tblHeader th').eq( $(this).index() ).text();
			var id = $('#biometricevent-info-table thead #tblHeader th').eq( $(this).index());
			
			 if (this.id.toLowerCase().indexOf("date") != -1) {
				$(this).html( '<div id="date_' + this.id+ '" style="background: #fff; cursor: pointer; padding: 5px 10px; border: 1px solid #ccc" class="no-print form-control">' +
						 													'<i ></i>' +
																			'<span style="padding: 10px;"></span>' +
																		   	'</div>' );
			} else if(this.id.toLowerCase().indexOf("function") != -1){
				$(this).html( '<select class="form-control header-text" id=search_' + title.replace(/ /g,'_') + '><option value="">All</option></select><i></i>' );
			} else{
				$(this).html( '<input id=search_' + title.replace(/ /g,'_') + ' type="text" class="form-control" placeholder="Search" +title />' );				 
			}

			//biometricId, externalId, eventId, binId, assignedSegmentId, status, phase, dataVersion, updateDateTime
			$('#search_Biometric_Id').addClass("numericTextBox");
			$('#search_Event_Id').addClass("numericTextBox");
			$('#search_Bin_Id').addClass("numericTextBox");
			$('#search_AssignedSegment_Id').addClass("numericTextBox");
			$('#search_DataVersion').addClass("numericTextBox");
		});	
		
				
		$('.btn-group').insertAfter('.ColVis').show();	// Swap position of Back botton with Show/ Hide button.
		// Setup - add a text input to each footer cell
	    $('#biometricevent-info-table tfoot th').each( function () {
	        var title = $(this).text();
	        $(this).html( '<input type="text" placeholder="Search '+title+'" />' );
	    } );

	    function getTemplateDetails(biometricId){
			var getTemplateDetailsUrl="<%=request.getContextPath()%>/secured/admin/bioevent/getTemplateDetails.htm?biometricId=" + biometricId;
			var jsonRequestData = {};
			
			$.ajax({
		            type : 'GET',
		            url : getTemplateDetailsUrl,
		            cache: false,
		            async: false,
					dataType: 'json',
					data : jsonRequestData,
					beforeSend: function(){
						//loader.show();
					},
					complete: function(){
						//loader.hide();
					},
		            success : function(result){
		            	var templateDetails = result.data;
		            	showTemplateDetail(templateDetails);
		            },
		            error : function(XMLHttpRequest, textStatus, errorThrown){
						if(XMLHttpRequest.status == 401){
							redirectLoginPage();
						}
		            	errorAlert("Template Details Error", "Template Details can not be loaded.");
		            }
		        });
			
		}
	    
		function triggerAutoRefresh(switchId){
			if(switchId.checked){
				runTimer(60000, autoRefreshBiometricEvents);
			}else{
				clearInterval(intervalID);
			}
		}
		
		function runTimer(interval, functionName) {
			intervalID = setInterval(functionName, interval);
		};
		
		function autoRefreshBiometricEvents(){
	        otable.ajax.reload( null, false );
	    }
		
		function resetSearchTextFields(){
            $("input[id^='search_']").each(function () {
                var id = this.id;
                $('#'+id).val('');
            });
        }
			
        function resetDropDownFields(){
            $("select[id^='search_']").each(function () {
                var id = this.id;
                $('select option[value=""]').prop("selected",true);
            });

        }
		
		function resetDateFields(){
            $("div[id^='date_search_']").each(function () {
                var id = this.id;
                $('#'+id+' span').html('From -  To <i class="fa fa-calendar"></i>');
            });

        }

        function resetAllFields(){
            resetSearchTextFields();
            resetDropDownFields();
			resetDateFields();
            drawBiometricEventInfoTable();//Redraw the table
        }
		
		function showTemplateDetail(templateDetails){
			$('#templateDetail').val(templateDetails);
			$('#templateDetailsModalPopup').modal("show");
			resizeModal();
		}

		drawBiometricEventInfoTable();
		
		initgetdateRange('date_search_updateDateTime_th');
</script>