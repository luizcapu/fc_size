package adtsahring.fc_size;

public class Folder {

	private Integer folderId;
	private Integer parentId;
	private Long currentSize;
	
	public Folder (Integer folderId, Integer parentId, Long currentSize) {
		this.setFolderId(folderId);
		this.setParentId(parentId);
		this.setCurrentSize(currentSize);
	}
	
	public Integer getFolderId() {
		return folderId;
	}
	public void setFolderId(Integer folderId) {
		this.folderId = folderId;
	}
	public Integer getParentId() {
		return parentId;
	}
	public void setParentId(Integer parentId) {
		this.parentId = parentId;
	}
	public Long getCurrentSize() {
		return currentSize;
	}
	public void setCurrentSize(Long currentSize) {
		this.currentSize = currentSize;
	}
	
	
	
}
