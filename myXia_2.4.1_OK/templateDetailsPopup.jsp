<div class="modal fade" id="templateDetailsModalPopup" role="dialog">
	<div class="modal-dialog" style="width: 60%;">
		<div class="modal-content resizableModal">
			<div class="modal-header">
				<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times</button>
				<h2 class="modal-title">Template Details</h2>
			</div>
			
			<div class="modal-body resizeContent">
				<div class="row">
					<div class="col-md-12 smart-form ">
						<section>
							<label class="textarea"><textarea rows="10" cols="20" id="templateDetail" class="resizeContent"></textarea>
							</label>
						</section>
					</div>
				</div>
			</div>
			<div class="modal-footer">
				<button type="button" class="btn btn-default btn-warning" title="Copy to Clipboard" onclick="copyToClipboard('#templateDetail')">Copy</button>
				<button type="button" class="btn btn-default btn-warning" onClick="closeModal(templateDetailsModalPopup);" id="closeBtn">Close</button>
			</div>
		</div>
		<!-- /.modal-content -->
	</div>
</div>
<style>

</style>
<script>


function copyToClipboard(elementId) {
	var jsonText = "";
	if(elementId == '#templateDetail'){
		jsonText = $(elementId).val();
	}else{
		jsonText = $(elementId)[0].innerText;
	}
	if (window.clipboardData && window.clipboardData.setData) {
        // Copy to Clipboard for IE / Edge
        return clipboardData.setData("Text", text); 
    } else if (document.queryCommandSupported && document.queryCommandSupported("copy")) {
        var textarea = document.createElement("textarea");
        textarea.textContent = jsonText;
        textarea.style.position = "fixed";
        document.body.appendChild(textarea);
        textarea.select();
        try {
            return document.execCommand("copy");
        } catch (ex) {
            console.warn("Copy to clipboard failed.", ex);
            return false;
        } finally {
            document.body.removeChild(textarea);
        }
    }
}
</script>